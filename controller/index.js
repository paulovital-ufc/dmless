const http = require('http');
http.globalAgent.keepAlive = true
const https = require('https');
https.globalAgent.keepAlive = true

const config = require(`${__dirname}/config/app-config.json`)
const faas = require(`${__dirname}/src/faas`)
const queue = require(`${__dirname}/src/queue`)

// Por causa dos experimentos
const persistence = require(`${__dirname}/src/persistence`)

// Executa o setup inicial do serviço
const setup = require(`${__dirname}/src/setup`)()
const consistentHashing = setup.hashConsistent

const redis = require('promise-redis')()
const express = require('express')

const client = redis.createClient()
const serverPort = 8080
const app = express()

const POST = 1
const DELETE = 2
const GET = 3

/* Tempo estimado para o cold start
 * O valor será atualizado com o passar do tempo à medida 
 * em que se obter os tempos de resposta do FaaS
 */
client.set('coldStartTime', 6000)
client.set('coldStartCount', 0) // Contador de cold starts
client.set('001_f', 0)
client.set('002_f', 0)
client.set('003_f', 0)
client.set('004_f', 0)
client.set('005_f', 0)

app.use(express.urlencoded({ extended: true }))

app.get(`/${config.APIResourceName}/:tenant/:key`, async (req, res) => {
    const tenant = req.params.tenant
    const key = req.params.key

    // Parâmetros úteis ao experimento
    const keyForHashing = (req.query.keyforhashing) ? true : false
    const loadStrategy = (req.query.loadstrategy) ? req.query.loadstrategy : '1'

    // Busca a chave no Redis
    let timestamp = await client.get(`${tenant}_${key}`)

    if (!timestamp) {
        // Ter um valor de timestamp armazenado no banco de chaves (redis) é condição
        // necessária para enviar a requisição de leitura (ou deleção) para o FaaS.
        // Não dá pra verificar se a informação obtida está ou não atualizada no FaaS sem esse dado.
        res.send({ value: null })

    } else {
        // A finalidade deste parâmetro é apenas servir aos experimentos
        const directBD = req.query.directbd
        let valueItem = null
        let resultValue = null
        let coldStartTime = 0

        if (directBD) {
            /* Acesso direto ao serviço de persistência do DMLess
             * O objetivo deste experimento é observar a latência ao usar
             * acesso direto ao banco versus usando a memória do FaaS como cache
             */
            try {
                valueItem = await persistence.getItem(key, config.appTable, tenant)
                resultValue = (valueItem.Item) ? valueItem.Item.value : null
                res.send({ value: resultValue })
            } catch (error) {
                res.sendStatus(500)
            }

        } else {
            //Verifica se o valor está armazenado em cache
            let cachedValue = await client.get(`__${tenant}_${key}`)
            if (cachedValue) {

                //console.log('----------------------------- CACHED')
                // O valor ainda estava no cache do Redis. Atende à requisição.
                res.send({ value: cachedValue })

            } else {
                // Verificando a instância de armazenamento através de hash consistente
                const hashNode = (keyForHashing) ? getHash(`${tenant}_${key}`) : getHash(tenant)

                let coldStart = await isColdStart(hashNode)
                let aux // Apenas para auxiliar ao esperar o resultado de funções com await

                if (coldStart) {
                    // Busca a informação diretamente no serviço de Persistência
                    try {
                        valueItem = await persistence.getItem(key, config.appTable, tenant)
                        resultValue = (valueItem.Item) ? valueItem.Item.value : null
                        // Atende à requisição
                        res.send({ value: resultValue })
                        //console.log('BD')

                    } catch (error) {
                        res.sendStatus(500)
                    }

                    // Faz cache do valor no Redis 
                    if (valueItem) {
                        coldStartTime = await client.get('coldStartTime')
                        // Faz cache do valor no redis e define o tempo de expiração em milissegundos ('PX')
                        client.set(`__${tenant}_${key}`, JSON.stringify(resultValue), 'PX', coldStartTime)
                    }

                    // Requisita a informação ao DMless para que ocorra o Cold Start
                    let start = new Date().getTime() // Inicio um contador que será usado para modificar o tempo de cold start

                    faas.invokeFunction(hashNode, {
                        "method": GET,
                        "tenant": tenant,
                        "key": key,
                        "timestamp": timestamp,
                        "loadStrategy": loadStrategy
                    }).then(async e => {
                        let end = new Date().getTime()

                        // Atualiza o tempo de cold start
                        refreshColdStartTime(end - start)
                        coldStartCount() // Acrescenta o contador de cold starts

                        // Acrescenta uma instância ao grupo de instâncias disponíveis (o que pode não ser verdade no FaaS)
                        aux = await coldStartControl(hashNode, 1)
                    })

                } else {

                    //Diminui uma instância do grupo de instâncias disponíveis
                    aux = await coldStartControl(hashNode, -1)

                    let invokeResult = await faas.invokeFunction(hashNode, {
                        "method": GET,
                        "tenant": tenant,
                        "key": key,
                        "timestamp": timestamp,
                        "loadStrategy": loadStrategy
                    })

                    // Acrescenta uma instância ao grupo de instâncias disponíveis
                    aux = await coldStartControl(hashNode, 1)

                    let response = JSON.parse(invokeResult.Payload)
                    try {
                        response = JSON.parse(response.body)
                        res.send(response)
                    } catch (error) {
                        console.log(response)
                        res.sendStatus(500)
                    }
                }
            }
        }
    }
})

app.post(`/${config.APIResourceName}/:tenant`, async (req, res) => {
    const tenant = req.params.tenant
    const key = (req.query.key) ? req.query.key : req.body.key
    const value = (req.query.key) ? req.body : req.body.value

    // Parâmetros úteis ao experimento
    const keyForHashing = (req.query.keyforhashing) ? true : false

    // A finalidade deste parâmetro é apenas servir aos experimentos
    const directBD = req.query.directbd

    if (!key || !value || !tenant) {
        // Dados obrigatórios em falta
        res.sendStatus(400)

    } else {

        // Timestamp que irá controlar o processo de atualizações
        // além de definir se um item lido está atualizado ou não
        const timestamp = new Date().getTime()

        if (directBD) {
            /* Acesso direto ao serviço de persistência do DMLess
             * O objetivo deste experimento é observar a latência ao usar
             * acesso direto ao banco versus usando a memória do FaaS como cache
             */
            try {
                persistence.saveItem(key, value, config.appTable, tenant, timestamp)
            } catch (error) {
                console.log(error)
                res.sendStatus(500)
            }

        } else {
            // Verificando a instância de armazenamento através de hash consistente
            const hashNode = (keyForHashing) ? getHash(`${tenant}_${key}`) : getHash(tenant)

            // Busca a URL da fila no Redis
            const fifoUrl = await client.get(`fifoUrl${hashNode}`)

            // Envia os dados pra fila
            if (fifoUrl) {
                let messageBody = JSON.stringify({
                    "method": POST,
                    "tenant": tenant,
                    "key": key,
                    "value": value,
                    "timestamp": timestamp
                })
                queue.sendMessage(messageBody, fifoUrl, tenant, timestamp.toString())
            }
        }

        // Salva/atualiza a chave no Redis
        client.set(`${tenant}_${key}`, timestamp)

        // Envia resposta de sucesso
        res.sendStatus(200)
    }
})

app.put(`/${config.APIResourceName}/:tenant`, async (req, res) => {

    ////// Este é o mesmo código da função de POST /////

    const tenant = req.params.tenant
    const key = (req.query.key) ? req.query.key : req.body.key
    const value = (req.query.key) ? req.body : req.body.value
    console.log(req.query.key)
    console.log(req.body.value)

    // Parâmetros úteis ao experimento
    const keyForHashing = (req.query.keyforhashing) ? true : false

    // A finalidade deste parâmetro é apenas servir aos experimentos
    const directBD = req.query.directbd

    if (!key || !value || !tenant) {
        // Dados obrigatórios em falta
        res.sendStatus(400)

    } else {

        // Timestamp que irá controlar o processo de atualizações
        // além de definir se um item lido está atualizado ou não
        const timestamp = new Date().getTime()

        if (directBD) {
            /* Acesso direto ao serviço de persistência do DMLess
             * O objetivo deste experimento é observar a latência ao usar
             * acesso direto ao banco versus usando a memória do FaaS como cache
             */
            try {
                persistence.saveItem(key, value, config.appTable, tenant, timestamp)
            } catch (error) {
                console.log(error)
                res.sendStatus(500)
            }

        } else {
            // Verificando a instância de armazenamento através de hash consistente
            const hashNode = (keyForHashing) ? getHash(`${tenant}_${key}`) : getHash(tenant)

            // Busca a URL da fila no Redis
            const fifoUrl = await client.get(`fifoUrl${hashNode}`)

            // Envia os dados pra fila
            if (fifoUrl) {
                let messageBody = JSON.stringify({
                    "method": POST,
                    "tenant": tenant,
                    "key": key,
                    "value": value,
                    "timestamp": timestamp
                })
                queue.sendMessage(messageBody, fifoUrl, tenant, timestamp.toString()).then(console.log)
                console.log(messageBody)
                console.log('hashNode')
            }
        }

        // Salva/atualiza a chave no Redis
        client.set(`${tenant}_${key}`, timestamp)

        // Envia resposta de sucesso
        res.sendStatus(200)
    }
})

app.delete(`/${config.APIResourceName}/:tenant/:key`, async (req, res) => {
    const tenant = req.params.tenant
    const key = req.params.key

    // Parâmetros úteis ao experimento
    const keyForHashing = (req.query.keyforhashing) ? true : false

    // Busca a chave no Redis
    let timestamp = await client.get(`${tenant}_${key}`)

    if (timestamp) {

        // A finalidade deste parâmetro é apenas servir aos experimentos
        const directBD = req.query.directbd

        if (directBD) {
            /* Acesso direto ao serviço de persistência do DMLess
             * O objetivo deste experimento é observar a latência ao usar
             * acesso direto ao banco versus usando a memória do FaaS como cache
             */
            try {
                persistence.deleteItem(key, config.appTable, tenant)
            } catch (error) {
                console.log(error)
                res.sendStatus(500)
            }

        } else {
            // Verificando a instância de armazenamento através de hash consistente
            const hashNode = (keyForHashing) ? getHash(`${tenant}_${key}`) : getHash(tenant)

            // Busca a URL da fila no Redis
            const fifoUrl = await client.get(`fifoUrl${hashNode}`)

            // Envia mensagem para fila
            if (fifoUrl) {
                timestamp = new Date().getTime()
                let messageBody = JSON.stringify({
                    "method": DELETE,
                    "tenant": tenant,
                    "key": key,
                    "timestamp": timestamp
                })
                queue.sendMessage(messageBody, fifoUrl, tenant, timestamp.toString())
            }
        }

        // Remove a chave do Redis
        client.del(`${tenant}_${key}`)
    }

    // Envia a resposta de sucesso
    res.sendStatus(200)
})

app.listen(serverPort, () => {
    console.log('Servidor executando na porta ' + serverPort)
})

function getHash(tenant_key) {
    // É importante implementar no futuro um algoritmo que tente distribuir 
    // os cliente igualmente no pool de funções.
    let number = tenant_key.match(/\d+/g)
    number = (number[0]) ? number[0] : 1
    return consistentHashing.getNode(`${tenant_key}${4*number}`)

    //return consistentHashing.getNode(tenant_key)
}

async function isColdStart(hashNode) {
    // Procura se há instâncias de função disponíveis
    let freeInstances = await client.get(`${hashNode}_f`)
    if(typeof freeInstances === 'undefined' || freeInstances === null){
        client.set(`${hashNode}_f`, 0)
    }
    freeInstances = (typeof freeInstances !== 'undefined' && freeInstances !== null) ? parseInt(freeInstances) : 0
    freeInstances = (freeInstances <= 0) ? 0 : freeInstances // Para evitar valores negativos para o contador de instâncias disponíveis
    // Se tem instancias disponiveis então não é cold start (ver o nome da função)
    return (freeInstances > 0) ? false : true
}

async function coldStartCount(){
    let coldStartCount = await client.get(`coldStartCount`)
    coldStartCount = parseInt(coldStartCount)
    console.log("QTD COLD START: ", coldStartCount)
    client.set(`coldStartCount`, coldStartCount + 1)
    console.log('')
}

async function coldStartControl(hashNode, add) {
    let freeInstances = await client.get(`${hashNode}_f`)
    freeInstances = parseInt(freeInstances)

    client.set(`${hashNode}_f`, ((freeInstances + add) < 0) ? 0 : (freeInstances + add))
}

async function refreshColdStartTime(responseTime) {
    if (responseTime) {
        let coldStartTime = await client.get('coldStartTime')
        coldStartTime = parseInt((parseInt(coldStartTime) + responseTime) / 2)
        client.set('coldStartTime', coldStartTime)
        console.log("TEMPO DE COLD START: ", coldStartTime)
    }
}