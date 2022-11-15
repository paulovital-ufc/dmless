/* SERVIÇO DE FILA DE MENSAGENS : SQS
 * Outros serviços podem ser usados (inclusive de outros provedores)
 * desde que as mesmas funcionalidades sejam providas por este módulo
 */

const config = require(__dirname + '/../config/app-config.json')
const AWS = require('aws-sdk');

// Para usar o AWS SQS 
const sqs = new AWS.SQS({ apiVersion: '2012-11-05', region: config.region });

function createQueue(sequenceName){
    let params = {
        QueueName: `${config.queueNamePrefix}${sequenceName}.fifo`,
        Attributes: {
            'MessageRetentionPeriod': '150',
            'VisibilityTimeout': '130',
            'FifoQueue': 'true'
        },
        tags: {
            'queueName': `${config.queueNamePrefix}${sequenceName}`,
        }
    }
    
    return new Promise((resolve, reject) => {
        try{
            sqs.createQueue(params, function(err, data) {
                if (err) reject(err) 
                else resolve(data)
            })
        }catch(error){
            reject(error)
        }
    })
}

function deleteQueue(queueUrl){
    let params = {
        QueueUrl: queueUrl
    }

    return new Promise((resolve, reject) => {
        try{
            sqs.deleteQueue(params, function(err, data) {
                if (err) reject(err)
                else resolve(data)
            });
        }catch(error){
            reject(error)
        }
    })
}

function listQueues(){
    let params = {};
    
    return new Promise((resolve, reject) => {
        try{
            sqs.listQueues(params, function(err, data) {
                if (err) reject(err)
                else resolve(data) 
            });
        }catch(error){
            reject(error)
        }
    })
}

function getQueueUrl(sequenceName){
    var params = {
        QueueName: `${config.queueNamePrefix}${sequenceName}.fifo`,
    }

    return new Promise((resolve, reject) => {
        try{
            sqs.getQueueUrl(params, function(err, data) {
                if (err) reject(err)
                else resolve(data)
            });
        }catch(error){
            reject(error)
        }
    })
}

function sendMessage(messageBody, queueUrl, messageGroupId, messageDeduplicationId){
    let params = {
        MessageBody: messageBody,
        QueueUrl: queueUrl,
        MessageDeduplicationId: messageDeduplicationId,
        MessageGroupId: messageGroupId,
    }
    
    return new Promise((resolve, reject) => {
        try{
            sqs.sendMessage(params, function(err, data) {
                if (err) reject(err)
                else resolve(data)
            });
        }catch(error){
            reject(error)
        }
    })
}

function getQueueArn(queueUrl){
    var params = {
        QueueUrl: queueUrl,
        AttributeNames: [
          'QueueArn',
        ]
    }
    return new Promise((resolve, reject) => {
        try{
            sqs.getQueueAttributes(params, function(err, data) {
                if (err) reject(err)
                else resolve(data)
            });
        }catch(error){
            reject(error)
        }
    })
}

module.exports = {
    getQueueArn,
    sendMessage,
    getQueueUrl,
    listQueues,
    deleteQueue,
    createQueue
}