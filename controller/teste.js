const ConsistentHashing = require('consistent-hashing')
const hashConsistent = new ConsistentHashing(["001"])
const poolSize = 3
let sequenceNumber = ''

for (let i = 2; i <= poolSize; i++) {
    sequenceNumber = '' + i
    sequenceNumber = sequenceNumber.padStart(3, 0)
    hashConsistent.addNode(sequenceNumber)
}


//console.log('cliente'.match( /\d+/g ))

for(let i = 1; i <= 5; i++){
    console.log(`client${i}`, getHash(`client${i}`))
}

function getHash(tenant){
    let number = tenant.match(/\d+/g)
    number = (number[0]) ? number[0] : 1

    //return hashConsistent.getNode(`${tenant}${21*number}`)
    return hashConsistent.getNode(`${tenant}${4*number}`)
    //return hashConsistent.getNode(`${tenant}`)
}

//console.log(hashConsistent)

//-----------------------------------------

// let count = 1
// let nodes = []
// while(true){
//     nodes = []
//     for(let i = 1; i <= 10; i++){
//         if(nodes.indexOf(hashConsistent.getNode(`cliente${i}${count*i}`)) === -1){
//             nodes.push(hashConsistent.getNode(`cliente${i}${count*i}`))
//         }
//     }
//     if(nodes.length === 10) break
//     ++count
// }
// console.log(count)

//////////////////////////////////////////////////
// const AWS = require('aws-sdk')
// var lambda = new AWS.Lambda({apiVersion: '2015-03-31', region: 'sa-east-1'});

// var params = {
//   FunctionName: "testeProxy", 
//   Payload: Buffer.from(JSON.stringify({})),
//   InvocationType: 'Event'
//  };
//  lambda.invoke(params, function(err, data) {
//    if (err) console.log(err, err.stack); // an error occurred
//    else     console.log(data);           // successful response
//    /*
//    data = {
//     Payload: <Binary String>, 
//     StatusCode: 200
//    }
//    */
//  });

///////////////////////////////

// (async () => {
// 	console.log(await getIP())
// })();

// function getIP(){
//   const publicIp = require('public-ip')
//   return new Promise((resolve, reject) => {
//     try{
//       publicIp.v4().then(ip => resolve(ip))
//     }catch(error){
//       reject(error)
//     }
//   })
// }

// (async () => {
//   const persistence = require(`${__dirname}/src/persistence`)

//   let valueItem = await getItem()
//   let resultValue = (valueItem.Item) ? valueItem.Item.value : null

//   console.log(resultValue)

//   async function getItem(){
//     let valueItem = await persistence.getItem('user10083f36', 'appData', 'client5')
//     return valueItem
//   }
// })()

// const fora = 10

// const array = [1,2,3,4,5,6,7,8,9,10]

// array.forEach(e => {
//   console.log(fora)
// })

const LRU = require("lru-cache")

const options = {
    max: (128) * 1000, // Memória disponível em KB para o cache
    length: function (value) { return getKBytes(value) },
    maxAge: 1000 * 60 * 60, // 1 hora para os objetos em cache
    stale: true,
    updateAgeOnGet: true
}

cache = new LRU(options)

//console.log(cache.length)

cache.set('cpf', '03339301344')
cache.set('cpf2', '03339301344')

//console.log(cache.length)

function getKBytes(string) {
    return Buffer.byteLength(JSON.stringify(string), 'utf8') / 1000
}

//console.log(cache.dump())
//console.log(cache[Object.getOwnPropertySymbols(cache)[8]].length);

const str = `[{ "k": "cpf2", "v": "03339301344", "e": 1668046222690 },{ "k": "cpf", "v": "03339301344", "e": 1668046222690 }]|Fim`

console.log(cache.load(JSON.parse(str.split('|Fim')[0])))
console.log(cache.dump())
