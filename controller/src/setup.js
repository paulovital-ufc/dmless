const config = require(`${__dirname}/../config/app-config.json`)
const persistence = require(`${__dirname}/persistence`)
const faas = require(`${__dirname}/faas`)
const queue = require(`${__dirname}/queue`)
const redis = require('promise-redis')()
const ConsistentHashing = require('consistent-hashing')

let hashConsistent = null

let poolSize = (config.pollSize > 0) ? config.pollSize : 1

// Imita o comportamento da função sleep de outras linguagens
function sleep(ms) {
    var start = new Date().getTime(), expire = start + ms;
    while (new Date().getTime() < expire) { }
    return;
}

const print = v => console.log(v)

module.exports = () => {
    configApplication()

    let sequenceNumber = ''

    print("Inicializando o Hash Consistente")
    hashConsistent = new ConsistentHashing(["001"])

    for (let i = 2; i <= poolSize; i++) {
        sequenceNumber = '' + i
        sequenceNumber = sequenceNumber.padStart(3, 0)
        hashConsistent.addNode(sequenceNumber)
    }

    return { hashConsistent }
}

async function configApplication(){
    let sequenceNumber = '' // Para receber as strings de sequência (001, 002, etc.)
    let valueItem = null // Usada para receber os valores das consultas ao DynamoDB

    //Cria a tabela que receberá os dados
    let isThereADataTable = true
    try{
        isThereADataTable = await persistence.checkTable(config.appTable)
    }catch(error){
        isThereADataTable = false
    }

    if(!isThereADataTable){
        print(`Criando tabela ${config.appTable} no DynamoDB`)
        persistence.createTable(config.appTable, true)
        sleep(2000) // Espera por dois segundos
    }

    //Cria a tabela de configuração no DynamoDB
    let isThereAConfigTable = true
    try{
        isThereAConfigTable = await persistence.checkTable(config.configTable)
    }catch(error){
        isThereAConfigTable = false
    }

    if(!isThereAConfigTable){
        // Se a tabela de configuração não existe, cria
        print(`Criando tabela ${config.configTable} no DynamoDB`)
        await persistence.createTable(config.configTable)
        // Espera por 30 segundos
        sleep(30000) 
    }else{
        // Se a tabela de configuração existe, busca-se o valor do poolSize
        print("Buscando as configurações no DynamoDB")
        valueItem = await persistence.getItem('poolSize', config.configTable)
        poolSize = (valueItem.Item) ? parseInt(valueItem.Item.value) : null
        if(!poolSize) poolSize = config.poolSize
    }

    let criarAte = '000' // Para controlar até onde os recursos são criados
    let count = poolSize // Essa variável será usada para controlar todos os laços abaixo
    while(count > 0){
        sequenceNumber = '' + count
        sequenceNumber = sequenceNumber.padStart(3, 0)
        // É feita uma verificação na tabela de configuração pra saber se os recursos da sequência já foram criados
        valueItem = await persistence.getItem(sequenceNumber, config.configTable)
        if(valueItem.Item){
            /* Se existir um item na tabela com chave igual a sequência 
             * significa que a partir dali os recursos já foram criados
             * pois a verificação é feita do maior para o menor
             */
            criarAte = sequenceNumber
            break
        }
        --count
    }

    /* Filas do SQS
     */
    count = poolSize
    let queues = [] // Para agrupar as informações de cada fila que deverão ser persistidas ao final na tabela de configuração
    while(count > 0){
        sequenceNumber = '' + count
        sequenceNumber = sequenceNumber.padStart(3, 0)

        // Como dito anteriormente, os recursos são criados observando a maior sequência até a menor.
        // A variável criarAte vai dizer até onde devem ser criados.
        if(sequenceNumber === criarAte) break

        print(`Criando a fila ${sequenceNumber}`)

        try{
            queue.createQueue(sequenceNumber).then(result => {
                if(result.QueueUrl){
                    queues.push({
                        'name': `${config.queueNamePrefix}${sequenceNumber}.fifo`,
                        'url': result.QueueUrl
                    })
                }
            })
        }catch(error){
            print(error)
            break
        }

        sleep(2000) // Espera por dois segundos
        --count
    }

    /* Funções do Lambda
     */
    count = poolSize
    while(count > 0){
        sequenceNumber = '' + count
        sequenceNumber = sequenceNumber.padStart(3, 0)

        if(sequenceNumber === criarAte) break

        try{
            print(`Criando função de leitura ${sequenceNumber}`)
            await faas.createFunction(sequenceNumber)
            sleep(2000)

            print(`Criando função de escrita ${sequenceNumber}`)
            await faas.createFunction(sequenceNumber, 'write')
            sleep(2000)

        }catch(error){
            print(error)
            break
        }
        --count
    }

    /* Buscam-se os valores de ARN de cada fila do SQS criada.
     * Infelizmente essa informação não é retornada quando a fila é criada.
     * O ARN é necessário para a criação de cada eventSourceMapping do Lambda.
     * A busca é feita aqui para dar tempo de todas as filas terem sido criadas
     * com sucesso.
     */
    count = poolSize
    let index = 0 // Para controlar o array das filas cuja contagem dos índices é do menor para o maior, diferentemente do valor de count
    while(count > 0){
        sequenceNumber = '' + count
        sequenceNumber = sequenceNumber.padStart(3, 0)

        if(sequenceNumber === criarAte) break

        print(`Buscando ARN da fila ${sequenceNumber}`)

        try{
            let arn = await queue.getQueueArn(queues[index].url)
            if(arn.Attributes && arn.Attributes.QueueArn){
                // Acrescenta o valor do ARN no array com os dados das filas
                queues[index].arn = arn.Attributes.QueueArn
            }
        }catch(error){
            print(error)
            break
        }
        ++index
        --count
    }

    /* EventSourceMappings
     * Cada um representa a associação entre uma função de escrita e uma fila do SQS. 
     * Depois de obter o UUID de cada eventSourceMapping, todos os dados reunidos 
     * até aqui (array queues) são inseridos na tabela de configurações.
     */
    count = poolSize
    let result = null
    index = 0
    while(count > 0){
        sequenceNumber = '' + count
        sequenceNumber = sequenceNumber.padStart(3, 0)

        if(sequenceNumber === criarAte) break

        print(`Criando EventSourceMapping ${sequenceNumber}`)

        try{
            result = await faas.createEventSourceMapping(sequenceNumber, queues[index].arn)
            if(result.UUID){
                persistence.saveItem(sequenceNumber, {
                    'fifo': queues[index].name,
                    'fifoUrl': queues[index].url,
                    'fifoArn': queues[index].arn,
                    'eventSourceMappingUUID': result.UUID
                }, config.configTable)
            }
        }catch(error){
            print(error)
            break
        }
        ++index
        --count
    }

    if(!isThereAConfigTable){
        /* Na primeira vez que a tabela de consfiguração é criada
         * o valor do poolSize é o que está no arquivo de configurações.
         * Nas próximas execuções do setup, valerá o que estiver na tabela.
         */
        await persistence.saveItem('poolSize', poolSize, config.configTable)
    }

    getQueueDetails(poolSize)
}

async function getQueueDetails(poolSize) {
    let valueItem = null
    let client = redis.createClient()
    let sequenceNumber = ''

    print(`Buscando as URLs das filas no DynamoDB`)

    let count = poolSize
    while(count > 0){
        sequenceNumber = '' + count
        sequenceNumber = sequenceNumber.padStart(3, 0)

        try{
            valueItem = await persistence.getItem(sequenceNumber, config.configTable)
            if (valueItem.Item) {
                // Salva a chave no Redis
                await client.set(`fifoUrl${sequenceNumber}`, valueItem.Item.value.fifoUrl)
            }
            sleep(1000)

        }catch(error){
            print(error)
            break
        }
        --count
    }
    client.quit()

    getKeysFromBD()
}

async function getKeysFromBD(){
    // Por conta da fase de testes as chaves são carregadas de um arquivo
    // O objetivo é reduzir custos com o DynamoDB (caso eu quisesse carregar o banco de chaves do DynamoDB ao invés do arquivo)
    const fs = require('fs')
    const data = fs.readFileSync(`${__dirname}/../files/for-redis.txt`, 'utf-8')
    const lines = data.split(/\r?\n/);
    
    let client = redis.createClient()

    lines.forEach(line => {
        const row = JSON.parse(line)
        client.set(`${row.tenant}_${row.key}`, row.timestamp)
    })

    client.quit()
}