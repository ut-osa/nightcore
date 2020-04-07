#pragma once

#include <nan.h>
#include <memory>

#include "base/std_span_polyfill.h"

namespace faas {
namespace worker_lib {
class Manager;
}  // namespace worker_lib
}  // namespace faas

class WorkerManager : public Nan::ObjectWrap {
public:
    static void Init(v8::Local<v8::Object> exports);

private:
    WorkerManager();
    ~WorkerManager();

    static Nan::Persistent<v8::Function> constructor;
    static void New(const Nan::FunctionCallbackInfo<v8::Value>& info);

    static void Start(const Nan::FunctionCallbackInfo<v8::Value>& info);
    static void OnGatewayIOError(const Nan::FunctionCallbackInfo<v8::Value>& info);
    static void OnWatchdogIOError(const Nan::FunctionCallbackInfo<v8::Value>& info);

    static void IsGrpcService(const Nan::FunctionCallbackInfo<v8::Value>& info);
    static void WatchdogInputPipeFd(const Nan::FunctionCallbackInfo<v8::Value>& info);
    static void WatchdogOutputPipeFd(const Nan::FunctionCallbackInfo<v8::Value>& info);
    static void GatewayIpcPath(const Nan::FunctionCallbackInfo<v8::Value>& info);

    static void SetSendGatewayDataCallback(const Nan::FunctionCallbackInfo<v8::Value>& info);
    static void SetSendWatchdogDataCallback(const Nan::FunctionCallbackInfo<v8::Value>& info);
    static void SetIncomingFuncCallCallback(const Nan::FunctionCallbackInfo<v8::Value>& info);
    static void SetIncomingGrpcCallCallback(const Nan::FunctionCallbackInfo<v8::Value>& info);
    static void SetOutcomingFuncCallCompleteCallback(const Nan::FunctionCallbackInfo<v8::Value>& info);

    static void OnRecvGatewayData(const Nan::FunctionCallbackInfo<v8::Value>& info);
    static void OnRecvWatchdogData(const Nan::FunctionCallbackInfo<v8::Value>& info);
    static void OnOutcomingFuncCall(const Nan::FunctionCallbackInfo<v8::Value>& info);
    static void OnOutcomingGrpcCall(const Nan::FunctionCallbackInfo<v8::Value>& info);
    static void OnIncomingFuncCallComplete(const Nan::FunctionCallbackInfo<v8::Value>& info);

    void SendGatewayDataCallback(std::span<const char> data);
    void SendWatchdogDataCallback(std::span<const char> data);
    void IncomingFuncCallCallback(uint32_t handle, std::span<const char> input);
    void IncomingGrpcCallCallback(uint32_t handle, std::string_view method, std::span<const char> request);
    void OutcomingFuncCallCompleteCallback(uint32_t handle, bool success, std::span<const char> output);

    std::unique_ptr<faas::worker_lib::Manager> inner_;
    Nan::Persistent<v8::Function> send_gateway_data_callback_;
    Nan::Persistent<v8::Function> send_watchdog_data_callback_;
    Nan::Persistent<v8::Function> incoming_func_call_callback_;
    Nan::Persistent<v8::Function> incoming_grpc_call_callback_;
    Nan::Persistent<v8::Function> outcoming_func_call_complete_callback_;
};
