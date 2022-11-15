const persistence = require(`${__dirname}/src/persistence`)

// Para saber quando houve um cold start
let isLoaded = false

// Função que iniciará a execução na instância do Lambda.
const main = function (event, context, callback) {
    context.callbackWaitsForEmptyEventLoop = true

    if (!isLoaded) {
        console.log('>>>>>>>> COLD START')
        // Defino a instância de função como carregada
        isLoaded = true
    }

    if (event.Records) {
        // Este é um evento do SQS
        const messages = []

        event.Records.forEach(msg => { 
            const payload = JSON.parse(msg.body)
            if (payload.timestamp) {
                messages.push({
                    'timestamp': new Number(payload.timestamp).valueOf(),
                    'payload': payload
                })
            }
        })

        messages.sort(compare)

        messages.forEach(function (msg) {
            const payload = msg.payload

            if (payload.method && payload.tenant && payload.key || (payload.method == 1 && (typeof payload.value !== "undefined") && payload.timestamp)) {
                /// A operação já está idempotent? ///
                // Para simplificação do DMLess não foi implementado nenhum mecanismo que garanta que um dado valor foi de fato persistido
                // Os eventos de escrita são assincronos de modo que seria mais custoso implementar um "ator" que ficasse supervisionando
                // as operações de escrita e fizesse retentativas em caso da falha. Sabe-se porém, que em casos de falha da instância
                // o próprio Lambda fas novas tentativas de execução. Nesse caso, a pergunta que fica é: em novas tentativas do Lambda
                // existe a possibilidade de causar inconsistencias no armazenamento do DMLess?
                // Presume-se que esse problema é resolvido por conta que as funções de escrita são monothread (modelo de ator).
                if (payload.method === 1) {
                    try {
                        // save ou update
                        persistence.saveItem(payload.tenant, payload.key, payload.value, payload.timestamp)
                        
                    } catch (error) {
                        console.log('>>>>>>>> ERROR [SAVE OR UPDATE]')
                        console.log(error)
                        throw error
                    }
                } else {
                    try {
                        // Apaga o item do banco
                        persistence.deleteItem(payload.tenant, payload.key)
                        
                    } catch (error) {
                        console.log('>>>>>>>> ERROR [DELETE]')
                        console.log(error)
                        throw error
                    }
                }
            }
        })

        return {}
    } 
}

module.exports.handler = main

/**
 * Funções auxiliares
 */

function compare(a, b) {
    if (a.timestamp > b.timestamp) return 1;
    if (b.timestamp > a.timestamp) return -1;

    return 0;
}