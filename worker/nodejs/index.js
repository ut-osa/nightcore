const addon = require('bindings')('addon')

class Context {
  constructor (engine, handle) {
    this.engine = engine
    this.handle = handle
  }

  invokeFunc (funcName, input, cb) {
    this.engine.invokeFunc(this.handle, funcName, input, cb)
  }

  grpcCall (service, method, request, cb) {
    this.engine.grpcCall(this.handle, service, method, request, cb)
  }
}

exports.serveForever = function (handlerFactory) {
  const engine = new addon.Engine()
  if (engine.isGrpcService()) {
    throw new Error('Can only call serveForever for normal function')
  }
  const handler = handlerFactory(engine.getFuncName())
  engine.start(function (handle, input, callback) {
    handler(new Context(engine, handle), input, function (err, output) {
      if (err) {
        callback(handle, false)
      } else {
        callback(handle, true, output)
      }
    })
  })
}

exports.serveGrpcService = function (service, implementation) {
  const engine = new addon.Engine()
  if (!engine.isGrpcService()) {
    throw new Error('Can only call serveGrpcService for gRPC service')
  }
  engine.start(function (handle, method, request, callback) {
    const methodDef = service[method]
    if (methodDef) {
      const handler = implementation[methodDef.originalName]
      if (handler) {
        handler({
          request: methodDef.requestDeserialize(request),
          faasContext: new Context(engine, handle),
        }, function (err, reply) {
          if (err) {
            callback(handle, false)
          } else {
            callback(handle, true, methodDef.responseSerialize(reply))
          }
        })
      } else {
        callback(new Error('Cannot find handler function for method ' + method))
      }
    } else {
      callback(new Error('Cannot process method ' + method))
    }
  })
}
