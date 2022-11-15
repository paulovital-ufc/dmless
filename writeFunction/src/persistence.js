const DynamoDB = require('aws-sdk/clients/dynamodb') 
const ddb = new DynamoDB.DocumentClient({ apiVersion: '2012-08-10', region: process.env.AWS_REGION });
const tableName = process.env.APP_TABLE

function saveItem(tenant, key, value, timestamp) {
    var params = {
        TableName: tableName,
        Item: {
            tenant: tenant,
            key: key,
            value: value,
            timestamp: timestamp
        }
    }
    return new Promise((resolve, reject) => {
        try {
            ddb.put(params, function (err, data) {
                if (err) reject(err)
                else resolve(data)
            })
        } catch (error) {
            reject(error)
        }
    })
}

function getItem(tenant, key) {
    var params = {
        TableName: tableName,
        Key: {
            tenant: tenant,
            key: key
        }
    }

    return new Promise((resolve, reject) => {
        try {
            ddb.get(params, function (err, data) {
                if (err) reject(err)
                else resolve(data)
            })
        } catch (error) {
            reject(error)
        }
    })
}

function deleteItem(tenant, key) {
    var params = {
        TableName: tableName,
        Key: {
            tenant: tenant,
            key: key
        }
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

function query(tenant){
    var params = {
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
    saveItem,
    getItem,
    deleteItem,
    query
}