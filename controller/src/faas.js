/* SERVIÇO DE FAAS: LAMBDA
 * Outros serviços podem ser usados (inclusive de outros provedores)
 * desde que as mesmas funcionalidades sejam providas por este módulo.
 */

const config = require(__dirname + '/../config/app-config.json')
const AWS = require('aws-sdk')
const { exec } = require("child_process")
const publicIp = require('public-ip')

const https = require('https')
const agent = new https.Agent({
  keepAlive: true
})

AWS.config.update({
  httpOptions: {
    agent
  }
})

// Para usar o AWS Lambda
let lambda = new AWS.Lambda({ 
  apiVersion: '2015-03-31',
  region: config.region,
  convertResponseTypes: false,
});

let public_ip_address
(async () => {
  public_ip_address = await getIP()
})()


/**
 * Cria uma instância de função que dependerá do parâmetro typeFunction
 * Poderá ser uma instância de escrita ou de leitrua
 */
function createFunction(sequenceName, typeFunction = 'read') {
  let params = {
    Code: {
      S3Bucket: config.applicationBucket,
      S3Key: (typeFunction === 'read' ) ? config.readFunctionCodePackage : config.writeFunctionCodePackage
    },
    FunctionName: (typeFunction === 'read' ) ? `${config.readFunctionPrefix}${sequenceName}` : `${config.writeFunctionPrefix}${sequenceName}`,
    MemorySize: config.cloudFunctionMemorySize,
    Handler: 'index.handler',
    Role: config.lambdaExecutionRole,
    Runtime: 'nodejs16.x',
    Description: (typeFunction === 'read' ) ? `Instancia de cache ${sequenceName}` : `Instancia de escrita ${sequenceName}`,
    Environment: {
      Variables: {
        'AWS_NODEJS_CONNECTION_REUSE_ENABLED': '1',
        'APP_TABLE': config.appTable,
        'PROXY_IP': public_ip_address
      }
    },
    Timeout: 10
  }

  return new Promise((resolve, reject) => {
    try {
      lambda.createFunction(params, function (err, data) {
        if (err) reject(err);
        else resolve(data);
      });
    } catch (error) {
      reject(error)
    }
  })
}

/**
 * Invoca uma função de forma síncrona (padrão) ou assíncrona.
 */
function invokeFunction(sequenceName, payload, sync = true, typeFunction = 'read') {
  var params = {
    FunctionName: (typeFunction === 'read' ) ? `${config.readFunctionPrefix}${sequenceName}` : `${config.writeFunctionPrefix}${sequenceName}`,
    Payload: Buffer.from(JSON.stringify(payload)),
    InvocationType: (!sync) ? 'Event' : 'RequestResponse'
  }
  return new Promise((resolve, reject) => {
    try {
      lambda.invoke(params, function (err, data) {
        if (err) reject(err)
        else resolve(data)
      })
    } catch (error) {
      reject(error)
    }
  })
}

function createEventSourceMapping(sequenceName, queueArn) {
  return new Promise((resolve, reject) => {
    var params = {
      BatchSize: 10,
      EventSourceArn: queueArn,
      FunctionName: `${config.writeFunctionPrefix}${sequenceName}`,
      Enabled: false // Cria desabilitado apenas para não afetar os testes. Mas o certo parece ser criar já habilitado
    }
    try {
      lambda.createEventSourceMapping(params, function (err, data) {
        if (err) reject(err)
        else resolve(data)
      })
    } catch (error) {
      reject(error)
    }
  })
}

function updateEventSourceMapping(UUID, enable = false) {
  var params = {
    UUID: UUID,
    Enabled: enable
  }
  return new Promise((resolve, reject) => {
    try {
      lambda.updateEventSourceMapping(params, function (err, data) {
        if (err) reject(err)
        else resolve(data)
      })
    } catch (error) {
      reject(error)
    }
  })
}

function deleteEventSourceMapping(UUID) {
  var params = {
    UUID: UUID
  }
  return new Promise((resolve, reject) => {
    try {
      lambda.deleteEventSourceMapping(params, function (err, data) {
        if (err) reject(err)
        else resolve(data)
      })
    } catch (error) {
      reject(error)
    }
  })
}

async function cliInvoke(sequenceName, payload, sync = true, typeFunction = 'read') {
  let functionName = (typeFunction === 'read' ) ? `${config.readFunctionPrefix}${sequenceName}` : `${config.writeFunctionPrefix}${sequenceName}`
  payload = JSON.stringify(payload)

  let command = `aws lambda invoke --function-name ${functionName} --payload '${payload}' out --cli-binary-format raw-in-base64-out > /dev/null && cat out`

  return await execPromise(command)
}

function execPromise(command){
  return new Promise((resolve, reject) => {
    exec(command, (error, stdout, stderr) => {
      if (error) {
        reject(error)
      }
      if (stderr) {
        reject(stderr)
      }
      resolve(stdout)
    })
  })
}

function getIP(){
  return new Promise((resolve, reject) => {
    try{
      publicIp.v4().then(ip => resolve(ip))
    }catch(error){
      reject(error)
    }
  })
}

function deleteFunctionConcurrency(sequenceName, typeFunction = 'read') {
  return new Promise((resolve, reject) => {
    var params = {
      FunctionName: (typeFunction === 'read' ) ? `${config.readFunctionPrefix}${sequenceName}` : `${config.writeFunctionPrefix}${sequenceName}`,
    }
    try {
      lambda.deleteFunctionConcurrency(params, function (err, data) {
        if (err) reject(err)
        else resolve(data)
      })
    } catch (error) {
      reject(error)
    }
  })
}

function getFunctionConcurrency(sequenceName, typeFunction = 'read') {
  return new Promise((resolve, reject) => {
    var params = {
      FunctionName: (typeFunction === 'read' ) ? `${config.readFunctionPrefix}${sequenceName}` : `${config.writeFunctionPrefix}${sequenceName}`,
    }
    try {
      lambda.getFunctionConcurrency(params, function (err, data) {
        if (err) reject(err)
        else resolve(data)
      })
    } catch (error) {
      reject(error)
    }
  })
}

module.exports = {
  invokeFunction,
  createFunction,
  deleteEventSourceMapping,
  updateEventSourceMapping,
  createEventSourceMapping,
  cliInvoke,
  deleteFunctionConcurrency,
  getFunctionConcurrency
}

//createFunction('001', 'write').then(console.log)

//createEventSourceMapping('001', 'arn:aws:sqs:sa-east-1:882669899363:readFunction001.fifo')

//deleteEventSourceMapping('c1d959a5-a616-49e4-84a7-69a1a4fe7fed').then(console.log)

// invokeFunction('001', {
//   "user_key": "client11_chave11",
//   "timestamp": 1615861148178}, 'read', true).then(console.log)

//updateEventSourceMapping('c3a63b48-e7a7-423f-8514-0436e3d66fc7', false)

//deleteFunctionConcurrency('003').then(console.log)