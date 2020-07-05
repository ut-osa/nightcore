const addon = require('bindings')('addon')

class Context {
  constructor (engine, handle) {
    this.engine = engine
    this.handle = handle
  }

  invokeFunc (funcName, input, cb) {
    this.engine.invokeFunc(this.handle, funcName, input, cb)
  }
}

exports.serveForever = function (handlerFactory) {
  const engine = new addon.Engine()
  const handler = handlerFactory(engine.getFuncName())
  if (engine.isGrpcService()) {
    // TODO
  } else {
    engine.start(function (handle, input, callback) {
      handler(new Context(engine, handle), input, function (err, output) {
        console.log(output.toString())
        if (err) {
          callback(handle, false)
        } else {
          callback(handle, true, output)
        }
      })
    })
  }
}
