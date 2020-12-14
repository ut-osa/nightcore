const faas = require('faas')

function fooHandler (context, input, cb) {
  context.invokeFunc('Bar', input, function (err, barOutput) {
    if (err) {
      cb(err)
      return
    }
    const output = Buffer.concat([Buffer.from('From function Bar: '), barOutput])
    cb(null, output)
  })
}

function barHandler (context, input, cb) {
  const output = Buffer.concat([input, Buffer.from(', World\n')])
  cb(null, output)
}

function main () {
  faas.serveForever(function (funcName) {
    if (funcName === 'Foo') {
      return fooHandler
    } else if (funcName === 'Bar') {
      return barHandler
    } else {
      console.error('Unknown function: ', funcName)
    }
  })
}

main()
