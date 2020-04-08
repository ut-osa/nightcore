import os
import logging
import asyncio

from . import _faas_native
__func_handler = None
__manager = _faas_native.WorkerManager()
__outcoming_func_calls = {}


class FaasError(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return self.message


class GatewayIpcProtocol(asyncio.Protocol):
    def __init__(self, manager, on_con_lost):
        self._manager = manager
        self._transport = None
        self._on_con_lost = on_con_lost 

    def connection_made(self, transport):
        self._transport = transport

    def data_received(self, data):
        self._manager.on_recv_gateway_data(data)

    def connection_lost(self, exc):
        self._manager.on_gateway_io_error('Disconnected: ' + str(exc))
        self._on_con_lost.set_result(True)


class WatchdogInputPipeProtocol(asyncio.Protocol):
    def __init__(self, manager):
        self._manager = manager
        self._transport = None

    def connection_made(self, transport):
        self._transport = transport

    def data_received(self, data):
        self._manager.on_recv_watchdog_data(data)

    def connection_lost(self, exc):
        self._manager.on_watchdog_io_error('Disconnected: ' + str(exc))


def _incoming_func_call_callback(manager):
    global __func_handler

    def task_complete_callback(handle):
        def func(task):
            success, output = False, b''
            e = task.exception()
            if e is not None:
                logging.warning('Function handler raises exception: %s' % str(e))
            elif isinstance(task.result(), bytes):
                success, output = True, task.result()
            else:
                logging.error('Function handler returns non-byte object')
            manager.on_incoming_func_call_complete(handle, success, output)
        return func

    def run_sync(handle, input_):
        success, output = False, b''
        try:
            output_ = __func_handler(input_)
            if isinstance(output_, bytes):
                success, output = True, output_
            else:
                logging.error('Function handler returns non-byte object')
        except Exception as e:
            logging.warning('Function handler raises exception: %s' % str(e))
        manager.on_incoming_func_call_complete(handle, success, output)

    def func(handle, input_):
        if asyncio.iscoroutinefunction(__func_handler):
            task = asyncio.create_task(__func_handler(input_))
            task.add_done_callback(task_complete_callback(handle))
        else:
            run_sync(handle, input_)

    return func


def _incoming_grpc_call_callback(manager):
    def func(handle, method, request):
        raise Exception('Not implemented')
    return func


def _outcoming_func_call_complete_callback(manager):
    def func(handle, success, output):
        if handle not in __outcoming_func_calls:
            logging.error('Cannot find handle %d in __outcoming_func_calls\n' % handle)
            return
        fut = __outcoming_func_calls.pop(handle)
        fut.set_result(output if success else None)
    return func


def set_func_handler(handler):
    global __func_handler
    if __func_handler is not None:
        raise FaasError('Function handler has already set')
    __func_handler = handler


async def invoke_func(func_name, input_):
    global __manager
    global __outcoming_func_calls
    handle = __manager.on_outcoming_func_call(func_name, input_)
    if handle is None:
        logging.warning('on_outcoming_func_call failed\n')
        return None
    fut = asyncio.get_running_loop().create_future()
    __outcoming_func_calls[handle] = fut
    return await fut


async def serve_forever():
    global __manager

    manager = __manager
    loop = asyncio.get_running_loop()

    on_gateway_con_lost = loop.create_future()
    gateway_transport, _ = await loop.create_unix_connection(
        lambda: GatewayIpcProtocol(manager, on_gateway_con_lost),
        path=manager.gateway_ipc_path)
    manager.set_send_gateway_data_callback(
        lambda data: gateway_transport.write(data))

    watchdog_input_transport, _ = await loop.connect_read_pipe(
        lambda: WatchdogInputPipeProtocol(manager),
        os.fdopen(manager.watchdog_input_pipe_fd, 'rb'))
    watchdog_output_transport, _ = await loop.connect_write_pipe(
        lambda: asyncio.Protocol(),
        os.fdopen(manager.watchdog_output_pipe_fd, 'wb'))
    manager.set_send_watchdog_data_callback(
        lambda data: watchdog_output_transport.write(data))

    if manager.is_grpc_service:
        manager.set_incoming_grpc_call_callback(_incoming_grpc_call_callback(manager))
    else:
        manager.set_incoming_func_call_callback(_incoming_func_call_callback(manager))
    manager.set_outcoming_func_call_complete_callback(
        _outcoming_func_call_complete_callback(manager))

    manager.start()
    await on_gateway_con_lost
