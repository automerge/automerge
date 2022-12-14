let { RedisSync } = require('.')
let clientId = `C${ rand(1000) }`
let docId = process.argv[2]

if (typeof docId !== 'string' || docId.length < 2 || docId.length > 20) {
  console.log("usage: node client.js DOCID")
  process.exit(1)
}

console.log("clientID is", clientId)
console.log("docID is", docId)

let init = (doc) => {
    doc.put("/","title", "token tracker")
    doc.putObject("/","tokens", [])
}

let sync = new RedisSync({ redis: "redis://", docId, clientId, init, update });
sync.connect()

function rand(max) {
  return Math.floor(Math.random() * max)
}

function update() {
    console.log("DOC STATE", sync.toJS())
}

function tweak() {
    if (rand(3) == 0) {
      sync.change((doc) => {
          let len = doc.length("/tokens")
          if (len + rand(10) > 20) {
            doc.delete("/tokens", rand(len))
          } else {
            doc.insert("/tokens", rand(len), rand(255))
          }
          doc.put("/", "winner", clientId)
      })
    }
}

setInterval(tweak, 3000)
