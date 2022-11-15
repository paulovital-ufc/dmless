const net = require('net')
const redis = require('promise-redis')()
const crypto = require("crypto")
const persistence = require(`${__dirname}/src/persistence`)
const config = require(`${__dirname}/config/app-config.json`)
const ConsistentHashing = require('consistent-hashing')
const LRU = require("lru-cache")

let hashConsistent = null

let poolSize = (config.pollSize > 0) ? config.pollSize : 1
let sequenceNumber = ''

hashConsistent = new ConsistentHashing(["001"])

for (let i = 2; i <= poolSize; i++) {
    sequenceNumber = '' + i
    sequenceNumber = sequenceNumber.padStart(3, 0)
    hashConsistent.addNode(sequenceNumber)
}

function getHash(key) {
    // É importante implementar no futuro um algoritmo que tente distribuir 
    // os cliente igualmente no pool de funções.
    let number = key.match(/\d+/g)
    number = (number[0]) ? number[0] : 1
    return hashConsistent.getNode(`${key}${4*number}`)

    //return consistentHashing.getNode(tenant_key)
}

const redisClient = redis.createClient()

// Cria o servidor de Proxy
var server = net.createServer()

// Emitido quando o proxy encerra todas as conexões
server.on('close', function () {
    redisClient.quit()
    console.log('Proxy closed !')
});

// Emitido quando há novas conexões com clientes
server.on('connection', function (socket) {
    
    socket.setEncoding('utf8')

    socket.setTimeout(20000, function () {
        socket.destroy()
    })

    let dataFromLambda = null
    let functionName = null

    socket.on('data', async function (data) {

        if(data.includes('_Send_')){
            console.log('SEND DATA: ', data, '\n')

            let sendProtocol = data.split('|')
            let hashOfData = await redisClient.get(`${sendProtocol[1]}_hash`)
            functionName = sendProtocol[1]

            if(sendProtocol[2] === hashOfData){
                /* O proxy tem a última versão dos dados para aquele cache
                 * O envio dos dados pelo Lambda não é necessário
                 * Encerra a conexão.
                 */
                console.log('Já tenho a versão mais recente dos dados')
                socket.end('Nop')
            }else{
                /* O proxy não tem a versão dos dados em questão.
                 */
                socket.write('Ok')
            }

        }else if(data.includes('_Get_')){
            console.log('GET DATA: ', data, '\n')

            let getProtocol = data.split('|')
            functionName = getProtocol[1]
            
            arrayDump = await redisClient.get(`${getProtocol[1]}_cache`)
            
            if(arrayDump){
                console.log('enviando dados para Lambda')
                socket.write(`${arrayDump}|Fim`)
                socket.end('Bye')
            }else{
                //console.log('sem dados para o lambda')
                // Se o proxy ainda não tem nenhuma versão dos dados
                // A conexão é encerrada
                socket.end('Bye')
            }
            

        }else{
            // Provavel recebimento de dados do Lambda
            dataFromLambda = dataFromLambda || ''
            dataFromLambda += data
            if(data.includes('|Fim')){
                // A transmissão chegou ao fim
                socket.end('Bye')
            }
        }
    })

    socket.on('error', function (error) {
        console.log('Error : ' + error)
    })

    socket.on('timeout', function () {
        dataFromLambda = null
        socket.end('Timed out!')
    })

    socket.on('end', function (data) {
    })

    socket.on('close', async function (error) {

        if(dataFromLambda && dataFromLambda.includes('|Fim')){

            dataFromLambda = dataFromLambda.split('|Fim')[0]
            
            const options = {
                max: 0, // Memória disponível em KB para o cache
                maxAge: 1000 * 60 * 60, // 1 hora para os objetos em cache
                stale: true,
                updateAgeOnGet: true
            }

            let cache = new LRU(options)
            
            try{
                cache.load(JSON.parse(dataFromLambda))
            }catch(error){
                console.log('Algum problema com os dados enviados para cache', error)
            }

            let length = cache[Object.getOwnPropertySymbols(cache)[8]].length
            
            console.log('cache length: ', length)
            console.log('functionName: ', functionName, '\n')

            if(length > 1 && functionName){
                
                let hash = crypto.createHash("sha256")
                .update(length + '')
                .digest("hex")

                await redisClient.set(`${functionName}_hash`, hash)
                await redisClient.set(`${functionName}_cache`, JSON.stringify(cache.dump()))
            }
        }else{
            //console.log('evento de get. nada há para salvar cache')
        }
    })

    setTimeout(function () {
        var isdestroyed = socket.destroyed
        //console.log('Socket destroyed:' + isdestroyed)
        socket.destroy()
    }, 3000000)

    //socket.write(oneMb)
})

// Quando um erro ocorrer. Emite 'close'.
server.on('error', function (error) {
    console.log('Error: ' + error)
})

// O servidor está escutando
server.on('listening', async function () {
    const clientes = [
        'client1',
        'client2',
        'client3',
        'client4', 
        'client5'
    ]

    await clientes.forEach(async c => {
        let partition = `readFunction${getHash(c)}`

        const options = {
            max: 0, // Memória disponível em KB para o cache
            maxAge: 1000 * 60 * 60, // 1 hora para os objetos em cache
            stale: true,
            updateAgeOnGet: true
        }
        
        let cache = new LRU(options)

        // Se já existem dados em cache, são acrescentados
        arrayDump = await redisClient.get(`${partition}_cache`)
            
        if(arrayDump){
            cache.load(JSON.parse(arrayDump))
        }

        await persistence.query(c, config.appTable).then(response => {
            //console.log(c)
            response.Items.forEach(item => {
                cache.set(`${item.tenant}_${item.key}`, { value: JSON.stringify(item.value), timestamp: item.timestamp })
            })
        })

        let length = cache[Object.getOwnPropertySymbols(cache)[8]].length

        let hash = crypto.createHash("sha256")
                .update(length + '')
                .digest("hex")

        await redisClient.set(`${partition}_hash`, hash)
        await redisClient.set(`${partition}_cache`, JSON.stringify(cache.dump()))
    })
    
    console.log('Server is listening!')
})

// Caso se deseje estabelecer o máximo de conexões possíveis
//server.maxConnections = 10;

//static port allocation
server.listen(2222)

// Caso se deseje estabelecer um timeout para o Proxy
// setTimeout(function () {
//     server.close()
// }, 5000000)