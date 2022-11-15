/* SERVIÇO DE PERSISTÊNCIA : DYNAMODB
 * Outros serviços podem ser usados (inclusive de outros provedores)
 * desde que as mesmas funcionalidades sejam providas por este módulo
 */
const config = require(`${__dirname}/../config/app-config.json`)
const AWS = require('aws-sdk')

let ddb = new AWS.DynamoDB.DocumentClient({ apiVersion: '2012-08-10', region: config.region })
let dynamodb = null

function createTable(tableName, dataTable = false) {
    dynamodb = (!dynamodb) ? new AWS.DynamoDB({ apiVersion: '2012-08-10', region: config.region }) : dynamodb

    const attrDef = []
    if(dataTable){
        attrDef.push({
            AttributeName: "tenant",
            AttributeType: "S"
        })
    }
    attrDef.push({
        AttributeName: "key",
        AttributeType: "S",
    })

    let keyS = []
    if(dataTable){
        keyS = [
            {
                AttributeName: "tenant",
                KeyType: "HASH"
            },
            {
                AttributeName: "key",
                KeyType: "RANGE"
            }
        ]
    }else{
        keyS.push({
            AttributeName: "key",
            KeyType: "HASH"
        })
    }

    let params = {
        AttributeDefinitions: attrDef,
        KeySchema: keyS,
        ProvisionedThroughput: {
            ReadCapacityUnits: 1,
            WriteCapacityUnits: 1
        },
        TableName: tableName,
        //BillingMode: 'PAY_PER_REQUEST',
    }

    return new Promise((resolve, reject) => {
        try {
            dynamodb.createTable(params, function (err, data) {
                if (err) reject(err)
                else resolve(data)
            })
        } catch (error) {
            reject(error)
        }
    })
}

function updateTable(tableName, provisionedThroughput = false, readCapacity = 5, writeCapacity = 5) {
    dynamodb = (!dynamodb) ? new AWS.DynamoDB({ apiVersion: '2012-08-10', region: config.region }) : dynamodb
    const params = {}
    if(provisionedThroughput){
        params.ProvisionedThroughput = {
            ReadCapacityUnits: readCapacity, 
            WriteCapacityUnits: writeCapacity
        } 
        params.BillingMode = 'PROVISIONED'
    }else{
        params.BillingMode = 'PAY_PER_REQUEST'
    }
    
    params.TableName = tableName

    return new Promise((resolve, reject) => {
        try {
            dynamodb.updateTable(params, function (err, data) {
                if (err) reject(err)
                else resolve(data)
            })
        } catch (error) {
            reject(error)
        }
    })
}

function checkTable(tableName) {
    dynamodb = (!dynamodb) ? new AWS.DynamoDB({ apiVersion: '2012-08-10', region: config.region }) : dynamodb

    let params = {
        TableName: tableName
    }

    return new Promise((resolve, reject) => {
        try {
            dynamodb.describeTable(params, function (err, data) {
                if (err) resolve(false)
                else resolve(data)
            })
        } catch (error) {
            reject(error)
        }
    })
}

function saveItem(key, value, tableName, tenant = null, timestamp = null) {
    const item = {
        key: key,
        value: value
    }
    if(tenant) item.timestamp = timestamp
    if(tenant) item.tenant = tenant

    return ddb.put({
        TableName: tableName,
        Item: item
    }).promise()
}

function deleteItem(key, tableName, tenant = null) {
    const objKey = {
        key: key
    }
    if(tenant) objKey.tenant = tenant

    let params = {
        TableName: tableName,
        Key: objKey
    }

    return new Promise((resolve, reject) => {
        try {
            ddb.delete(params, function (err, data) {
                if (err) reject(err)
                else resolve(data)
            })
        } catch (error) {
            reject(error)
        }
    })
}

function getItem(key, tableName, tenant = null) {
    const objKey = {
        key: key
    }
    if(tenant) objKey.tenant = tenant

    let params = {
        TableName: tableName,
        Key: objKey
    }

    return new Promise((resolve, reject) => {
        ddb.get(params, function (err, data) {
            if (err) reject(err)
            else resolve(data)
        })
    })
}

function query(tenant, tableName){
    let params = {
        KeyConditionExpression: "#Tenant = :Tenant",
        ExpressionAttributeNames: {
            "#Tenant": "tenant"
        },
        ExpressionAttributeValues: {
            ":Tenant": tenant
        },
        TableName: tableName
    }

    return new Promise((resolve, reject) => {
        try{
            ddb.query(params, function(err, data) {
                if (err) reject(err)
                else resolve(data)
           })
        }catch(error){
            reject(error)
        }
    })
}

module.exports = {
    getItem,
    deleteItem,
    saveItem,
    checkTable,
    createTable,
    query
}