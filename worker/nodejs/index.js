const net = require('net')
const fs = require('fs')

const addon = require('bindings')('addon')
const _manager = new addon.WorkerManager()

let _funcHandler = null
const _outcomingFuncCalls = {}

exports.setFuncHandler = function (funcHandler) {
  if (_manager.isGrpcService()) {
    throw new Error('Cannot call setFuncHandler for gRPC service')
  }
  _funcHandler = funcHandler
}

exports.registerGrpcService = function (service, implementation) {
  if (!_manager.isGrpcService()) {
    throw new Error('Can only call registerGrpcService for gRPC service')
  }
  _funcHandler = function (method, request, callback) {
    const methodDef = service[method]
    if (methodDef) {
      const handler = implementation[methodDef.originalName]
      if (handler) {
        handler({
          request: methodDef.requestDeserialize(request)
        }, function (err, reply) {
          if (err) {
            callback(err)
          } else {
            callback(null, methodDef.responseSerialize(reply))
          }
        })
      } else {
        callback(new Error('Cannot find handler function for method ' + method))
      }
    } else {
      callback(new Error('Cannot process method ' + method))
    }
  }
}

function grpcCall (service, method, input, callback) {
  const handle = _manager.onOutcomingGrpcCall(service, method, input)
  if (handle) {
    _outcomingFuncCalls[handle] = callback
  } else {
    callback(new Error('grpcCall failed'))
  }
}

exports.createGrpcClient = function (service) {
  const result = {}
  for (const method in service) {
    const methodDef = service[method]
    const pathParts = methodDef.path.split('/')
    const serviceName = pathParts[1]
    result[methodDef.originalName] = function (request, callback) {
      grpcCall(serviceName, method, methodDef.requestSerialize(request), function (err, output) {
        if (err) {
          callback(err)
        } else {
          callback(null, methodDef.responseDeserialize(output))
        }
      })
    }
  }
  return result
}

exports.invokeFunc = function (funcName, input, callback) {
  const handle = _manager.onOutcomingFuncCall(funcName, input)
  if (handle) {
    _outcomingFuncCalls[handle] = callback
  } else {
    callback(new Error('invokeFunc failed'))
  }
}

exports.start = function () {
  if (typeof _funcHandler !== 'function') {
    throw new Error('Function handler is not registered')
  }

  const gatewayConn = net.createConnection(_manager.gatewayIpcPath())
  gatewayConn.on('data', function (data) {
    _manager.onRecvGatewayData(data)
  })
  gatewayConn.on('error', function (err) {
    _manager.onGatewayIOError(err.toString())
  })
  _manager.setSendGatewayDataCallback(function (data) {
    gatewayConn.write(data)
  })

  const watchdogInputPipe = fs.createReadStream('watchdog_input', {
    fd: _manager.watchdogInputPipeFd()
  })
  watchdogInputPipe.on('error', function (err) {
    _manager.onWatchdogIOError(err.toString())
  })
  watchdogInputPipe.on('data', function (data) {
    _manager.onRecvWatchdogData(data)
  })
  const watchdogOutputPipe = fs.createWriteStream('watchdog_output', {
    fd: _manager.watchdogOutputPipeFd()
  })
  watchdogOutputPipe.on('error', function (err) {
    _manager.onWatchdogIOError(err.toString())
  })
  _manager.setSendWatchdogDataCallback(function (data) {
    watchdogOutputPipe.write(data)
  })

  const incomingFuncCallCompleteCallback = function (handle) {
    return function (err, output) {
      if (err) {
        _manager.onIncomingFuncCallComplete(handle, false)
      } else {
        _manager.onIncomingFuncCallComplete(handle, true, output)
      }
    }
  }

  if (_manager.isGrpcService()) {
    _manager.setIncomingGrpcCallCallback(function (handle, method, request) {
      _funcHandler(method, request, incomingFuncCallCompleteCallback(handle))
    })
  } else {
    _manager.setIncomingFuncCallCallback(function (handle, input) {
      _funcHandler(input, incomingFuncCallCompleteCallback(handle))
    })
  }

  _manager.setOutcomingFuncCallCompleteCallback(function (handle, success, output) {
    const callback = _outcomingFuncCalls[handle]
    if (callback) {
      if (success) {
        callback(null, output)
      } else {
        callback(new Error('Function call failed'))
      }
      delete _outcomingFuncCalls[handle]
    }
  })

  _manager.start()
}
