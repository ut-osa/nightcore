#pragma once

#include "base/common.h"
#include "common/uv.h"
#include "utils/object_pool.h"
#include "worker/event_driven_worker.h"

#include <napi.h>

namespace faas {
namespace nodejs {

class Engine : public Napi::ObjectWrap<Engine> {
public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    explicit Engine(const Napi::CallbackInfo& info);
    ~Engine();

private:
    Napi::Env env_;
    std::unique_ptr<worker_lib::EventDrivenWorker> worker_;
    Napi::FunctionReference handler_;

    uv_loop_t* uv_loop_;
    utils::SimpleObjectPool<uv_poll_t> uv_poll_pool_;
    std::unordered_map<uv_poll_t*, int> uv_poll_to_fds_;
    std::unordered_map<int, uv_poll_t*> fd_to_uv_polls_;

    std::unordered_map</* handle */ int64_t, Napi::FunctionReference>
        outgoing_func_call_cbs_;

    void StartInternal(Napi::Function handler);

    Napi::Value IsGrpcService(const Napi::CallbackInfo& info);
    Napi::Value GetFuncName(const Napi::CallbackInfo& info);
    Napi::Value Start(const Napi::CallbackInfo& info);
    Napi::Value InvokeFunc(const Napi::CallbackInfo& info);
    Napi::Value GrpcCall(const Napi::CallbackInfo& info);
    static Napi::Value IncomingFuncCallFinished(const Napi::CallbackInfo& info);

    void AddWatchFdReadable(int fd);
    void RemoveWatchFdReadable(int fd);
    void OnIncomingFuncCall(int64_t handle, std::string_view method, std::span<const char> request);
    void OnOutgoingFuncCallComplete(int64_t handle, bool success, std::span<const char> output);

    void RemovePoll(uv_poll_t* uv_poll);

    DECLARE_UV_POLL_CB_FOR_CLASS(FdEvent);
    DECLARE_UV_CLOSE_CB_FOR_CLASS(PollClose);

    DISALLOW_COPY_AND_ASSIGN(Engine);
};

}  // namespace nodejs
}  // namespace faas
