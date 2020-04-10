#define __FAAS_NODE_ADDON_SRC
#include "worker_manager.h"

#include "worker/lib/manager.h"

namespace {
std::span<const char> node_buffer_to_span(const v8::Local<v8::Value>& buffer) {
    if (node::Buffer::HasInstance(buffer)) {
        return std::span<const char>(node::Buffer::Data(buffer), node::Buffer::Length(buffer));
    } else {
        return std::span<const char>();
    }
}

std::string v8_string_to_std_string(const v8::Local<v8::Value>& str) {
    Nan::Utf8String utf8_string(str);
    return std::string(*utf8_string);
}
}

Nan::Persistent<v8::Function> WorkerManager::constructor;

WorkerManager::WorkerManager() : inner_(new faas::worker_lib::Manager()) {}

WorkerManager::~WorkerManager() {
    send_gateway_data_callback_.Reset();
    send_watchdog_data_callback_.Reset();
    incoming_func_call_callback_.Reset();
    incoming_grpc_call_callback_.Reset();
    outcoming_func_call_complete_callback_.Reset();
}

void WorkerManager::Init(v8::Local<v8::Object> exports) {
    v8::Isolate* isolate = exports->GetIsolate();
    Nan::HandleScope scope;
    v8::Local<v8::Context> context = isolate->GetCurrentContext();

    // Prepare constructor template
    v8::Local<v8::FunctionTemplate> tpl = Nan::New<v8::FunctionTemplate>(New);
    tpl->SetClassName(Nan::New("WorkerManager").ToLocalChecked());
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    // Prototype
    Nan::SetPrototypeMethod(tpl, "start", Start);
    Nan::SetPrototypeMethod(tpl, "onGatewayIOError", OnGatewayIOError);
    Nan::SetPrototypeMethod(tpl, "onWatchdogIOError", OnWatchdogIOError);

    Nan::SetPrototypeMethod(tpl, "isGrpcService", IsGrpcService);
    Nan::SetPrototypeMethod(tpl, "watchdogInputPipeFd", WatchdogInputPipeFd);
    Nan::SetPrototypeMethod(tpl, "watchdogOutputPipeFd", WatchdogOutputPipeFd);
    Nan::SetPrototypeMethod(tpl, "gatewayIpcPath", GatewayIpcPath);

    Nan::SetPrototypeMethod(tpl, "setSendGatewayDataCallback", SetSendGatewayDataCallback);
    Nan::SetPrototypeMethod(tpl, "setSendWatchdogDataCallback", SetSendWatchdogDataCallback);
    Nan::SetPrototypeMethod(tpl, "setIncomingFuncCallCallback", SetIncomingFuncCallCallback);
    Nan::SetPrototypeMethod(tpl, "setIncomingGrpcCallCallback", SetIncomingGrpcCallCallback);
    Nan::SetPrototypeMethod(tpl, "setOutcomingFuncCallCompleteCallback", SetOutcomingFuncCallCompleteCallback);

    Nan::SetPrototypeMethod(tpl, "onRecvGatewayData", OnRecvGatewayData);
    Nan::SetPrototypeMethod(tpl, "onRecvWatchdogData", OnRecvWatchdogData);
    Nan::SetPrototypeMethod(tpl, "onOutcomingFuncCall", OnOutcomingFuncCall);
    Nan::SetPrototypeMethod(tpl, "onOutcomingGrpcCall", OnOutcomingGrpcCall);
    Nan::SetPrototypeMethod(tpl, "onIncomingFuncCallComplete", OnIncomingFuncCallComplete);

    constructor.Reset(tpl->GetFunction(context).ToLocalChecked());
    exports->Set(context,
                 Nan::New("WorkerManager").ToLocalChecked(),
                 tpl->GetFunction(context).ToLocalChecked());
}

void WorkerManager::New(const Nan::FunctionCallbackInfo<v8::Value>& info) {
    Nan::HandleScope scope;
    if (info.IsConstructCall()) {
        WorkerManager* obj = new WorkerManager();
        obj->Wrap(info.This());
        info.GetReturnValue().Set(info.This());
    } else {
        Nan::ThrowError("WorkerManager() must be called as constructor");
    }
}

void WorkerManager::Start(const Nan::FunctionCallbackInfo<v8::Value>& info) {
    Nan::HandleScope scope;
    if (info.Length() != 0) {
        Nan::ThrowTypeError("start() must be called without arguments");
        return;
    }
    v8::Local<v8::Context> context = info.GetIsolate()->GetCurrentContext();
    WorkerManager* obj = ObjectWrap::Unwrap<WorkerManager>(info.Holder());
    obj->inner_->Start();
}

void WorkerManager::OnGatewayIOError(const Nan::FunctionCallbackInfo<v8::Value>& info) {
    Nan::HandleScope scope;
    if (info.Length() != 1 || (!info[0]->IsNumber() && !info[0]->IsString())) {
        Nan::ThrowTypeError("onGatewayIOError() must be called with errno or error message");
        return;
    }
    v8::Local<v8::Context> context = info.GetIsolate()->GetCurrentContext();
    WorkerManager* obj = ObjectWrap::Unwrap<WorkerManager>(info.Holder());
    if (info[0]->IsNumber()) {
        int32_t errnum = info[0]->Int32Value(context).FromJust();
        obj->inner_->OnGatewayIOError(errnum);
    } else {
        obj->inner_->OnGatewayIOError(v8_string_to_std_string(info[0]));
    }
}

void WorkerManager::OnWatchdogIOError(const Nan::FunctionCallbackInfo<v8::Value>& info) {
    Nan::HandleScope scope;
    if (info.Length() != 1 || (!info[0]->IsNumber() && !info[0]->IsString())) {
        Nan::ThrowTypeError("onGatewayIOError() must be called with errno or error message");
        return;
    }
    v8::Local<v8::Context> context = info.GetIsolate()->GetCurrentContext();
    WorkerManager* obj = ObjectWrap::Unwrap<WorkerManager>(info.Holder());
    if (info[0]->IsNumber()) {
        int32_t errnum = info[0]->Int32Value(context).FromJust();
        obj->inner_->OnWatchdogIOError(errnum);
    } else {
        obj->inner_->OnWatchdogIOError(v8_string_to_std_string(info[0]));
    }
}

void WorkerManager::IsGrpcService(const Nan::FunctionCallbackInfo<v8::Value>& info) {
    Nan::HandleScope scope;
    if (info.Length() > 0) {
        Nan::ThrowTypeError("isGrpcService() must be called no argument");
        return;
    }
    WorkerManager* obj = ObjectWrap::Unwrap<WorkerManager>(info.Holder());
    info.GetReturnValue().Set(Nan::New(obj->inner_->is_grpc_service()));
}

void WorkerManager::WatchdogInputPipeFd(const Nan::FunctionCallbackInfo<v8::Value>& info) {
    Nan::HandleScope scope;
    if (info.Length() > 0) {
        Nan::ThrowTypeError("watchdogInputPipeFd() must be called no argument");
        return;
    }
    WorkerManager* obj = ObjectWrap::Unwrap<WorkerManager>(info.Holder());
    info.GetReturnValue().Set(Nan::New(int32_t{obj->inner_->watchdog_input_pipe_fd()}));
}

void WorkerManager::WatchdogOutputPipeFd(const Nan::FunctionCallbackInfo<v8::Value>& info) {
    Nan::HandleScope scope;
    if (info.Length() > 0) {
        Nan::ThrowTypeError("watchdogOutputPipeFd() must be called no argument");
        return;
    }
    WorkerManager* obj = ObjectWrap::Unwrap<WorkerManager>(info.Holder());
    info.GetReturnValue().Set(Nan::New(int32_t{obj->inner_->watchdog_output_pipe_fd()}));
}

void WorkerManager::GatewayIpcPath(const Nan::FunctionCallbackInfo<v8::Value>& info) {
    Nan::HandleScope scope;
    if (info.Length() > 0) {
        Nan::ThrowTypeError("gatewayIpcPath() must be called no argument");
        return;
    }
    WorkerManager* obj = ObjectWrap::Unwrap<WorkerManager>(info.Holder());
    info.GetReturnValue().Set(Nan::New(std::string(obj->inner_->gateway_ipc_path())).ToLocalChecked());
}

void WorkerManager::SetSendGatewayDataCallback(const Nan::FunctionCallbackInfo<v8::Value>& info) {
    Nan::HandleScope scope;
    if (info.Length() != 1 || !info[0]->IsFunction()) {
        Nan::ThrowTypeError("setSendGatewayDataCallback() must be called with callback function");
        return;
    }
    v8::Local<v8::Function> callback = info[0].As<v8::Function>();
    WorkerManager* obj = ObjectWrap::Unwrap<WorkerManager>(info.Holder());
    obj->send_gateway_data_callback_.Reset(callback);
    obj->inner_->SetSendGatewayDataCallback([obj] (std::span<const char> data) {
        obj->SendGatewayDataCallback(data);
    });
}

void WorkerManager::SetSendWatchdogDataCallback(const Nan::FunctionCallbackInfo<v8::Value>& info) {
    Nan::HandleScope scope;
    if (info.Length() != 1 || !info[0]->IsFunction()) {
        Nan::ThrowTypeError("setSendWatchdogDataCallback() must be called with callback function");
        return;
    }
    v8::Local<v8::Function> callback = info[0].As<v8::Function>();
    WorkerManager* obj = ObjectWrap::Unwrap<WorkerManager>(info.Holder());
    obj->send_watchdog_data_callback_.Reset(callback);
    obj->inner_->SetSendWatchdogDataCallback([obj] (std::span<const char> data) {
        obj->SendWatchdogDataCallback(data);
    });
}

void WorkerManager::SetIncomingFuncCallCallback(const Nan::FunctionCallbackInfo<v8::Value>& info) {
    Nan::HandleScope scope;
    if (info.Length() != 1 || !info[0]->IsFunction()) {
        Nan::ThrowTypeError("setIncomingFuncCallCallback() must be called with callback function");
        return;
    }
    v8::Local<v8::Function> callback = info[0].As<v8::Function>();
    WorkerManager* obj = ObjectWrap::Unwrap<WorkerManager>(info.Holder());
    obj->incoming_func_call_callback_.Reset(callback);
    obj->inner_->SetIncomingFuncCallCallback([obj] (uint32_t handle, std::span<const char> input) {
        obj->IncomingFuncCallCallback(handle, input);
    });
}

void WorkerManager::SetIncomingGrpcCallCallback(const Nan::FunctionCallbackInfo<v8::Value>& info) {
    Nan::HandleScope scope;
    if (info.Length() != 1 || !info[0]->IsFunction()) {
        Nan::ThrowTypeError("setIncomingGrpcCallCallback() must be called with callback function");
        return;
    }
    v8::Local<v8::Function> callback = info[0].As<v8::Function>();
    WorkerManager* obj = ObjectWrap::Unwrap<WorkerManager>(info.Holder());
    obj->incoming_grpc_call_callback_.Reset(callback);
    obj->inner_->SetIncomingGrpcCallCallback([obj] (uint32_t handle, std::string_view method,
                                                    std::span<const char> request) {
        obj->IncomingGrpcCallCallback(handle, method, request);
    });
}

void WorkerManager::SetOutcomingFuncCallCompleteCallback(const Nan::FunctionCallbackInfo<v8::Value>& info) {
    Nan::HandleScope scope;
    if (info.Length() != 1 || !info[0]->IsFunction()) {
        Nan::ThrowTypeError("setOutcomingFuncCallCompleteCallback() must be called with callback function");
        return;
    }
    v8::Local<v8::Function> callback = info[0].As<v8::Function>();
    WorkerManager* obj = ObjectWrap::Unwrap<WorkerManager>(info.Holder());
    obj->outcoming_func_call_complete_callback_.Reset(callback);
    obj->inner_->SetOutcomingFuncCallCompleteCallback([obj] (uint32_t handle, bool success,
                                                             std::span<const char> output,
                                                             bool* reclaim_output_later) {
        obj->OutcomingFuncCallCompleteCallback(handle, success, output);
    });
}

void WorkerManager::OnRecvGatewayData(const Nan::FunctionCallbackInfo<v8::Value>& info) {
    Nan::HandleScope scope;
    if (info.Length() != 1 || !node::Buffer::HasInstance(info[0])) {
        Nan::ThrowTypeError("onRecvGatewayData() must be called with data buffer");
        return;
    }
    v8::Local<v8::Context> context = info.GetIsolate()->GetCurrentContext();
    WorkerManager* obj = ObjectWrap::Unwrap<WorkerManager>(info.Holder());
    obj->inner_->OnRecvGatewayData(node_buffer_to_span(info[0]));
}

void WorkerManager::OnRecvWatchdogData(const Nan::FunctionCallbackInfo<v8::Value>& info) {
    Nan::HandleScope scope;
    if (info.Length() != 1 || !node::Buffer::HasInstance(info[0])) {
        Nan::ThrowTypeError("onRecvWatchdogData() must be called with data buffer");
        return;
    }
    v8::Local<v8::Context> context = info.GetIsolate()->GetCurrentContext();
    WorkerManager* obj = ObjectWrap::Unwrap<WorkerManager>(info.Holder());
    v8::Local<v8::Value> data = info[0];
    obj->inner_->OnRecvWatchdogData(node_buffer_to_span(info[0]));
}

void WorkerManager::OnOutcomingFuncCall(const Nan::FunctionCallbackInfo<v8::Value>& info) {
    Nan::HandleScope scope;
    if (info.Length() != 2 || !info[0]->IsString() || !node::Buffer::HasInstance(info[1])) {
        Nan::ThrowTypeError("onOutcomingFuncCall() must be called with (func_name, input)");
        return;
    }
    v8::Local<v8::Context> context = info.GetIsolate()->GetCurrentContext();
    WorkerManager* obj = ObjectWrap::Unwrap<WorkerManager>(info.Holder());
    uint32_t handle;
    if (obj->inner_->OnOutcomingFuncCall(v8_string_to_std_string(info[0]),
                                         node_buffer_to_span(info[1]), &handle)) {
        info.GetReturnValue().Set(Nan::New(handle));
    }
}

void WorkerManager::OnOutcomingGrpcCall(const Nan::FunctionCallbackInfo<v8::Value>& info) {
    Nan::HandleScope scope;
    if (info.Length() != 3 || !info[0]->IsString() || !info[1]->IsString() || !node::Buffer::HasInstance(info[2])) {
        Nan::ThrowTypeError("onOutcomingGrpcCall() must be called with (service, method, request)");
        return;
    }
    v8::Local<v8::Context> context = info.GetIsolate()->GetCurrentContext();
    WorkerManager* obj = ObjectWrap::Unwrap<WorkerManager>(info.Holder());
    uint32_t handle;
    if (obj->inner_->OnOutcomingGrpcCall(v8_string_to_std_string(info[0]), v8_string_to_std_string(info[1]),
                                         node_buffer_to_span(info[2]), &handle)) {
        info.GetReturnValue().Set(Nan::New(handle));
    }
}

void WorkerManager::OnIncomingFuncCallComplete(const Nan::FunctionCallbackInfo<v8::Value>& info) {
    Nan::HandleScope scope;
    if ((info.Length() != 2 && info.Length() != 3) || !info[0]->IsNumber() || !info[1]->IsBoolean()) {
        Nan::ThrowTypeError("onIncomingFuncCallComplete() must be called with (handle, success, [output])");
        return;
    }
    v8::Local<v8::Context> context = info.GetIsolate()->GetCurrentContext();
    WorkerManager* obj = ObjectWrap::Unwrap<WorkerManager>(info.Holder());
    uint32_t handle = info[0]->Int32Value(context).FromJust();
    bool success = info[1]->BooleanValue(context).FromJust();
    std::span<const char> output = node_buffer_to_span(info[2]);
    obj->inner_->OnIncomingFuncCallComplete(handle, success, output);
}

void WorkerManager::SendGatewayDataCallback(std::span<const char> data) {
    Nan::HandleScope scope;
    v8::Local<v8::Function> callback = Nan::New(send_gateway_data_callback_);
    v8::Local<v8::Value> argv[] = {
        Nan::CopyBuffer(data.data(), data.size()).ToLocalChecked()
    };
    Nan::MakeCallback(Nan::GetCurrentContext()->Global(), callback, 1, argv);
}

void WorkerManager::SendWatchdogDataCallback(std::span<const char> data) {
    Nan::HandleScope scope;
    v8::Local<v8::Function> callback = Nan::New(send_watchdog_data_callback_);
    v8::Local<v8::Value> argv[] = {
        Nan::CopyBuffer(data.data(), data.size()).ToLocalChecked()
    };
    Nan::MakeCallback(Nan::GetCurrentContext()->Global(), callback, 1, argv);
}

void WorkerManager::IncomingFuncCallCallback(uint32_t handle, std::span<const char> input) {
    Nan::HandleScope scope;
    v8::Local<v8::Function> callback = Nan::New(incoming_func_call_callback_);
    v8::Local<v8::Value> argv[] = {
        Nan::New(handle),
        Nan::CopyBuffer(input.data(), input.size()).ToLocalChecked()
    };
    Nan::MakeCallback(Nan::GetCurrentContext()->Global(), callback, 2, argv);
}

void WorkerManager::IncomingGrpcCallCallback(uint32_t handle, std::string_view method,
                                             std::span<const char> request) {
    Nan::HandleScope scope;
    v8::Local<v8::Function> callback = Nan::New(incoming_grpc_call_callback_);
    v8::Local<v8::Value> argv[] = {
        Nan::New(handle),
        Nan::New(method.data(), method.size()).ToLocalChecked(),
        Nan::CopyBuffer(request.data(), request.size()).ToLocalChecked()
    };
    Nan::MakeCallback(Nan::GetCurrentContext()->Global(), callback, 3, argv);
}

void WorkerManager::OutcomingFuncCallCompleteCallback(uint32_t handle, bool success,
                                                      std::span<const char> output) {
    Nan::HandleScope scope;
    v8::Local<v8::Function> callback = Nan::New(outcoming_func_call_complete_callback_);
    v8::Local<v8::Value> argv[] = {
        Nan::New(handle),
        Nan::New(success),
        Nan::CopyBuffer(output.data(), output.size()).ToLocalChecked()
    };
    Nan::MakeCallback(Nan::GetCurrentContext()->Global(), callback, 3, argv);
}
