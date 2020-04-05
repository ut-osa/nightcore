var net = require('net')
var fs = require('fs')
var addon = require('bindings')('addon')

function Worker (funcHandler) {
  this._manager = new addon.WorkerManager()
  this._funcHandler = funcHandler
  this._outcomingFuncCalls = {}
}

Worker.prototype.serve = function () {
  var manager = this._manager
  var funcHandler = this._funcHandler
  var worker = this

  var gatewayConn = net.createConnection(manager.gatewayIpcPath())
  gatewayConn.on('data', function (data) {
    manager.onRecvGatewayData(data)
  })
  gatewayConn.on('error', function (err) {
    manager.onGatewayIOError(err.toString())
  })
  manager.setSendGatewayDataCallback(function (data) {
    gatewayConn.write(data)
  })

  var watchdogInputPipe = fs.createReadStream('watchdog_input', {
    fd: manager.watchdogInputPipeFd()
  })
  watchdogInputPipe.on('error', function (err) {
    manager.onWatchdogIOError(err.toString())
  })
  watchdogInputPipe.on('data', function (data) {
    manager.onRecvWatchdogData(data)
  })
  var watchdogOutputPipe = fs.createWriteStream('watchdog_output', {
    fd: manager.watchdogOutputPipeFd()
  })
  watchdogOutputPipe.on('error', function (err) {
    manager.onWatchdogIOError(err.toString())
  })
  manager.setSendWatchdogDataCallback(function (data) {
    watchdogOutputPipe.write(data)
  })

  manager.setIncomingFuncCallCallback(function (handle, input) {
    var context = {
      invokeFunc: function (funcName, input, callback) {
        worker.invokeFunc(funcName, input, callback)
      }
    }
    funcHandler(input, context, function (err, output) {
      if (err) {
        manager.onIncomingFuncCallComplete(handle, false)
      } else {
        manager.onIncomingFuncCallComplete(handle, true, output)
      }
    })
  })

  manager.setOutcomingFuncCallCompleteCallback(function (handle, success, output) {
    var callback = worker._outcomingFuncCalls[handle]
    if (callback) {
      if (success) {
        callback(null, output)
      } else {
        callback(new Error('invokeFunc failed'))
      }
      delete worker._outcomingFuncCalls[handle]
    }
  })

  manager.start()
}

Worker.prototype.invokeFunc = function (funcName, input, callback) {
  var handle = this._manager.onOutcomingFuncCall(funcName, input)
  if (handle) {
    this._outcomingFuncCalls[handle] = callback
  } else {
    callback(new Error('invokeFunc failed'))
  }
}

exports.Worker = Worker
