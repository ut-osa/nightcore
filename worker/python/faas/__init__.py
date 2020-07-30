import os
import logging
import asyncio
from collections import namedtuple

from . import _faas_native


class Error(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return self.message


class GrpcChannelWrapper(object):
    def __init__(self, context):
        self._context = context
    
    def unary_unary(self, path, request_serializer, response_deserializer):
        async def fn(request):
            parts = path.split('/')
            service = parts[1].strip()
            method = parts[2].strip()
            response_bytes = await self._context.grpc_call(
                service, method, request_serializer(request))
            return response_deserializer(response_bytes)
        return fn


class Context(object):
    def __init__(self, engine, handle):
        self._engine = engine
        self._handle = handle
        self.grpc_channel = GrpcChannelWrapper(self)

    async def invoke_func(self, func_name, input_):
        return await self._engine.invoke_func(self._handle, func_name, input_)

    async def grpc_call(self, service, method, request):
        return await self._engine.grpc_call(self._handle, service, method, request)


class Engine(object):
    def __init__(self):
        self._worker = _faas_native.Worker()
        self._outgoing_func_calls = {}
        self._watching_fds = {}
        self._set_callbacks()
    
    def func_name(self):
        if self._worker.is_grpc_service:
            return self._worker.grpc_service_name
        else:
            return self._worker.func_name

    async def start(self, handler):
        self._handler = handler
        self._loop = asyncio.get_running_loop()
        self._finished_fut = self._loop.create_future()
        self._worker.start()
        await self._finished_fut

    def _set_callbacks(self):
        def watch_fd_readable_cb(fd_):
            fd = os.fdopen(fd_, 'rb')
            self._watching_fds[fd_] = fd
            self.add_watch_fd_readable(fd)
        def stop_watch_fd_cb(fd_):
            if fd_ in self._watching_fds:
                fd = self._watching_fds.pop(fd_)
                self.remove_watch_fd_readable(fd)
            if len(self._watching_fds) == 0:
                self._finished_fut.set_result(True)
        def incoming_func_call_cb(handle, method, request):
            self.on_incoming_func_call(handle, method, request)
        def outgoing_func_call_complete_cb(handle, success, output):
            self.on_outgoing_func_call_complete(handle, success, output)
        self._worker.set_watch_fd_readable_callback(watch_fd_readable_cb)
        self._worker.set_stop_watch_fd_callback(stop_watch_fd_cb)
        self._worker.set_incoming_func_call_callback(incoming_func_call_cb)
        self._worker.set_outgoing_func_call_complete_callback(
            outgoing_func_call_complete_cb)
    
    def _run_handler_async(self, handle, method, input_):
        def done_callback(task):
            success, output = False, b''
            e = task.exception()
            if e is not None:
                logging.warning('Function handler raises exception: %s' % str(e))
            elif isinstance(task.result(), bytes):
                success, output = True, task.result()
            else:
                logging.error('Function handler returns non-byte object')
            self._worker.on_func_execution_finished(handle, success, output)
        context = Context(self, handle)
        if self._worker.is_grpc_service:
            task = asyncio.create_task(self._handler(context, method, input_))
        else:
            task = asyncio.create_task(self._handler(context, input_))
        task.add_done_callback(done_callback)

    def _run_handler_sync(self, handle, method, input_):
        success, output = False, b''
        try:
            context = Context(self, handle)
            if self._worker.is_grpc_service:
                output_ = self._handler(context, method, input_)
            else:
                output_ = self._handler(context, input_)
            if isinstance(output_, bytes):
                success, output = True, output_
            else:
                logging.error('Function handler returns non-byte object')
        except Exception as e:
            logging.warning('Function handler raises exception: %s' % str(e))
        self._worker.on_func_execution_finished(handle, success, output)

    def invoke_func(self, parent_handle, func_name, input_):
        fut = self._loop.create_future()
        handle = self._worker.new_outgoing_func_call(parent_handle, func_name, input_)
        if handle is None:
            fut.set_exception(Error('new_outgoing_func_call failed'))
        else:
            self._outgoing_func_calls[handle] = fut
        return fut

    def grpc_call(self, parent_handle, service, method, request):
        fut = self._loop.create_future()
        handle = self._worker.new_outgoing_grpc_call(parent_handle, service, method, request)
        if handle is None:
            fut.set_exception(Error('new_outgoing_grpc_call failed'))
        else:
            self._outgoing_func_calls[handle] = fut
        return fut

    def on_incoming_func_call(self, handle, method, input_):
        if asyncio.iscoroutinefunction(self._handler):
            self._run_handler_async(handle, method, input_)
        else:
            self._run_handler_sync(handle, method, input_)

    def on_outgoing_func_call_complete(self, handle, success, output):
        if handle in self._outgoing_func_calls:
            fut = self._outgoing_func_calls.pop(handle)
            if success:
                fut.set_result(output)
            else:
                fut.set_exception(Error('invoke_func failed'))

    def add_watch_fd_readable(self, fd):
        def func():
            self._worker.on_fd_readable(fd.fileno())
        self._loop.add_reader(fd, func)

    def remove_watch_fd_readable(self, fd):
        self._loop.remove_reader(fd)


def serve_forever(handler_factory):
    engine = Engine()
    asyncio.run(engine.start(handler_factory(engine.func_name())))
