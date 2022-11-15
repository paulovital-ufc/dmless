const persistence = require(`${__dirname}/src/persistence`)

const LRU = require("lru-cache")

const net = require('net')

// Política de cache: LRU
let cache = null

// Para saber quando houve um cold start
let isLoaded = false

// Função que iniciará a execução na instância do Lambda.
const main = async function (event, context, callback) {
    // O retorno da função tem de esperar pelo processamento da pilha de execução do event loop?
    // Antes de usar socket (para o proxy) fazia sentido não ter de esperar (valor false)
    // O carregamento dos dados do proxy, porém, deve ser feito apenas uma vez quando a função for carregada pelo Lambda a primeira vez.
    context.callbackWaitsForEmptyEventLoop = (!isLoaded) ? true : false

    if (event.ping) {
        // Pode ser usado para "acordar" a função como estratégia de redução de cold start
        // É uma estratégia questionável já que o provedor vai acordar apenas as funções
        // necessárias para atender a resquisição PING and PONG (o que pode significar apenas uma função).
        // A melhor abordagem para ter funções pré-carregadas e reduzir o cold start é usar 
        // a capacidade reservada de funções, porém o custo dessa abordagem é bem maior.
        console.log('>>>>>>>> PONG')
        return {}
    }

    if (!isLoaded) {
        console.log('>>>>>>>> COLD START')

        // Inicia o objeto de cache

        // Quanto de memória há para o cache?
        const memoryAlocatedInMB = 90 // Memória ocupada pelo código da função
        const memoryMaxInMB = context.memoryLimitInMB // Memória total da função

        const options = {
            max: (memoryMaxInMB - memoryAlocatedInMB) * 1000, // Memória disponível em KB para o cache
            length: function (value) { return getKBytes(value) },
            maxAge: 1000 * 60 * 60, // 1 hora para os objetos em cache
            stale: true,
            updateAgeOnGet: true
        }

        cache = new LRU(options)
    }

    if (!event.tenant || !event.key || !event.timestamp) {
        callback(null, response({
            status: 'error',
            msg: 'Todos os parâmetros devem ser fornecidos'
        }))
        return
    }

    const tenant_key = `${event.tenant}_${event.key}`
    const timestamp = event.timestamp
    const cacheValue = cache.get(tenant_key)
    const loadStrategy = event.loadStrategy

    if (!isLoaded ||
        !cacheValue ||
        (cacheValue && cacheValue.timestamp != timestamp)) {

        /* Em cold start ou em caso de dado desatualizado
         * é preciso ir no banco
         */
        let valueItem = null
        let resultValue = null
        try {
            valueItem = await persistence.getItem(event.tenant, event.key)
            resultValue = (valueItem.Item) ? valueItem.Item.value : null

            console.log('>>>>>>>> DynamoDB')

        } catch (error) {
            console.log('>>>>>>>> ERROR [GET]')
            console.log(error)
            callback(null, {
                statusCode: 500,
                body: JSON.stringify(error),
            })
            throw error
        }

        if (resultValue) {
            // Atualiza o cache
            cache.set(tenant_key, {
                value: resultValue,
                timestamp: valueItem.Item.timestamp
            })
        }

        // Responde a requisição.
        callback(null, response({
            value: resultValue
        }))

        if (!isLoaded) {
            /* Executa estratégia de cache de forma assíncrona
             * Seria bom executar a mesma estratégia caso apenas o dado esteja desatualizado?
             * A estratégia atual busca 1MB das chaves do cliente em caso de cold start
             * ou se o cliente ainda não tiver sido carregado em memória
             */
            try {
                cacheStrategy(event.tenant, loadStrategy, context.functionName)
            } catch (error) {
                console.log('>>>>>>>> ERROR [STRATEGY]')
                console.log(error)
                throw error
            }
        }

        // Defino a instância de função como carregada
        isLoaded = true

    } else {
        // O armazenamento em memória é suficiente para atender a requisição
        callback(null, response({
            value: cacheValue.value
        }))
        console.log('>>>>>>>> Cache')
    }

}

module.exports.handler = main

/**
 * Funções auxiliares
 */

// Retorna o tamanho de uma string em KB
function getKBytes(string) {
    return Buffer.byteLength(JSON.stringify(string), 'utf8') / 1000
}

// A estratégia de cache atual é trazer todas as chaves de um único cliente para a memória
function cacheStrategy(tenant, loadStrategy, functionName) {
    if (loadStrategy == '1') {
        loadStrategy = getRandomInt(1, 5).toString() // Obriga o uso limitado da estratégia atual
        switch (loadStrategy) {
            case '6':
                try {
                    console.log('>>>>>>>> Strategy')
                    persistence.query(tenant).then(response => {
                        response.Items.forEach(item => {
                            cache.set(`${item.tenant}_${item.key}`, { value: item.value, timestamp: item.timestamp })
                        })
                        sendDataToProxy(functionName)
                    })
                } catch (error) {
                    throw error
                }
                return
            // case 4:
            //     getDataFromProxy(functionName);
            //     return
            default:
                //getDataFromProxy(functionName);
                return
        }
    }
}

function response(objResponse) {
    return {
        statusCode: 200,
        body: JSON.stringify(objResponse),
    }
}

function getRandomInt(min, max) {
    min = Math.ceil(min);
    max = Math.floor(max);
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

async function sendDataToProxy(functionName) {
    try {
        // Captura um array com os dados em cache
        const arrayDump = JSON.stringify(cache.dump())

        // Gera um hash da quantidade atual dos dados em cache
        const crypto = require("crypto")
        const hash = crypto.createHash("sha256")
            .update(arrayDump.length + '') // Não é a melhor maneira de gerar esse hash (pode ter x objetos de tenants diferentes)
            .digest("hex")
        // Comunica-se com o servidor de proxy para envio dos dados
        const client = new net.Socket()
        client.connect({
            port: 2222,
            host: process.env.PROXY_IP
        })

        client.on('connect', function () {
            console.log('SEND DATA TO PROXY')

            // Enviando a solicitação de envio de dados para o proxy
            // Envia também o hash dos dados em cache
            // O proxy irá comparar o hash enviado com o dele para saber se
            // precisa mesmo dessa versão dos dados
            client.write(`_Send_|${functionName}|${hash}`)
        })

        client.setEncoding('utf8')

        client.on('data', function (data) {
            switch (data) {
                case 'Ok':
                    /* O proxy respondeu 'Ok'
                     * Significa que os dados do cache da função 
                     * podem ser enviados
                     */
                    client.write(`${arrayDump}|Fim`)
                    break
                case 'Nop':
                    /* O proxy respondeu 'Nop'
                     * Significa que ele já tem essa versão dos dados.
                     */
                    break
                default:
                    /* Ao que parece a resposta do servidor foi inesperada.
                     * Nada há para ser feito neste caso.
                     */
                    console.log(`Proxy answer`, data)
            }
        })

        client.on('close', function (error) {
            console.log('Connection closed by the proxy')
            client.destroy()
        })

    } catch (error) {
        console.log('>>>>>>>> ERROR [PROXY SEND]', error)
    }
}

async function getDataFromProxy(functionName) {
    try {
        let client = new net.Socket();
        client.connect({
            port: 2222,
            host: process.env.PROXY_IP
        })

        client.on('connect', function () {
            console.log('GET DATA FROM PROXY')
            client.write(`_Get_|${functionName}`)
        })

        client.setEncoding('utf8')

        let dataFromProxy = null
        client.on('data', function (data) {
            dataFromProxy = dataFromProxy || ''
            dataFromProxy += data // Os dados são recebidos em pedaços (buffer TCP?)

            if (data.includes('|Fim') || data.includes('Bye')) {
                // Transmissão terminou
                client.destroy()
                //client.end('Bye')
            }
        })

        client.on('close', function (error) {
            if (!client.destroyed) {
                client.destroy()
            }
            if (dataFromProxy && dataFromProxy.includes('|Fim')) {
                dataFromProxy = dataFromProxy.split('|Fim')[0]
                console.log('Has |Fim')
                try {
                    console.log('Saving cache object')
                    cache.load(JSON.parse(dataFromProxy))

                    //let length = cache[Object.getOwnPropertySymbols(cache)[8]].length
                    // console.log('DEPOIS DE GET: ', length)
                    // console.log('client1_user83ab0aef: ', cache.get('client1_user83ab0aef'))
                    // console.log('client2_user33479eed: ', cache.get('client2_user33479eed'))
                    // console.log('client3_userd92335b0: ', cache.get('client3_userd92335b0'))
                    // console.log('client4_userafe1a1c4: ', cache.get('client4_userafe1a1c4'))
                    // console.log('client5_user68104886: ', cache.get('client5_user68104886'))

                } catch (error) {
                    console.log('Algum problema com os dados enviados para cache', error)
                }
            }
        })

    } catch (error) {
        console.log('>>>>>>>> ERROR [PROXY GET]', error)
    }
}