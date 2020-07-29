#define __FAAS_NODE_ADDON_SRC
#include "base/logging.h"
#include "utils/env_variables.h"
#include "engine.h"

namespace faas {
namespace nodejs {

Napi::Object Engine::Init(Napi::Env env, Napi::Object exports) {
    logging::Init(utils::GetEnvVariableAsInt("FAAS_VLOG_LEVEL", 0));

    Napi::Function func = DefineClass(
        env, "Engine",
        {
            InstanceMethod("isGrpcService", &Engine::IsGrpcService),
            InstanceMethod("getFuncName", &Engine::GetFuncName),
            InstanceMethod("start", &Engine::Start),
            InstanceMethod("invokeFunc", &Engine::InvokeFunc),
            InstanceMethod("grpcCall", &Engine::GrpcCall)
        }
    );

    Napi::FunctionReference* constructor = new Napi::FunctionReference();
    *constructor = Napi::Persistent(func);
    env.SetInstanceData(constructor);

    exports.Set("Engine", func);
    return exports;
}

Engine::Engine(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<Engine>(info),
      env_(info.Env()) {
    if (info.Length() != 0) {
        Napi::TypeError::New(env_, "Engine constructor takes no argument")
            .ThrowAsJavaScriptException();
        return;
    }

    napi_status status = napi_get_uv_event_loop(env_, &uv_loop_);
    if (status != napi_ok) {
        LOG(FATAL) << "Failed to get uv_loop from napi_env ";
    }

    worker_.reset(new worker_lib::EventDrivenWorker());
    worker_->SetWatchFdReadableCallback([this] (int fd) {
        AddWatchFdReadable(fd);
    });
    worker_->SetStopWatchFdCallback([this] (int fd) {
        RemoveWatchFdReadable(fd);
    });
    worker_->SetIncomingFuncCallCallback([this] (int64_t handle, std::string_view method,
                                                 std::span<const char> request) {
        OnIncomingFuncCall(handle, method, request);
    });
    worker_->SetOutgoingFuncCallCompleteCallback([this] (int64_t handle, bool success,
                                                         std::span<const char> output) {
        OnOutgoingFuncCallComplete(handle, success, output);
    });
}

Engine::~Engine() {}

void Engine::StartInternal(Napi::Function handler) {
    handler_ = Napi::Persistent(handler);
    worker_->Start();
}

Napi::Value Engine::IsGrpcService(const Napi::CallbackInfo& info) {
    if (info.Length() != 0) {
        Napi::TypeError::New(info.Env(), "isGrpcService takes no argument")
            .ThrowAsJavaScriptException();
        return info.Env().Undefined();
    }
    return Napi::Boolean::New(info.Env(), worker_->is_grpc_service());
}

Napi::Value Engine::GetFuncName(const Napi::CallbackInfo& info) {
    if (info.Length() != 0) {
        Napi::TypeError::New(info.Env(), "getFuncName takes no argument")
            .ThrowAsJavaScriptException();
        return info.Env().Undefined();
    }
    std::string result;
    if (worker_->is_grpc_service()) {
        result.assign(worker_->grpc_service_name());
    } else {
        result.assign(worker_->func_name());
    }
    return Napi::String::New(info.Env(), result);
}

Napi::Value Engine::Start(const Napi::CallbackInfo& info) {
    if (info.Length() != 1 || !info[0].IsFunction()) {
        Napi::TypeError::New(info.Env(), "start takes one function argument")
            .ThrowAsJavaScriptException();
        return info.Env().Undefined();
    }
    StartInternal(info[0].As<Napi::Function>());
    return info.Env().Undefined();
}

namespace {
static double encode_to_double(int64_t value) {
    static_assert(sizeof(double) == sizeof(int64_t));
    double result;
    memcpy(&result, &value, sizeof(double));
    return result;
}

static int64_t decode_from_double(double value) {
    static_assert(sizeof(double) == sizeof(int64_t));
    int64_t result;
    memcpy(&result, &value, sizeof(int64_t));
    return result;
}
}

Napi::Value Engine::InvokeFunc(const Napi::CallbackInfo& info) {
    if (info.Length() != 4) {
        Napi::TypeError::New(info.Env(), "invokeFunc takes 4 arguments")
            .ThrowAsJavaScriptException();
        return info.Env().Undefined();
    }
    if (!info[0].IsNumber()) {
        Napi::TypeError::New(info.Env(), "The 1st argument should be a number")
            .ThrowAsJavaScriptException();
        return info.Env().Undefined();
    }
    if (!info[1].IsString()) {
        Napi::TypeError::New(info.Env(), "The 2nd argument should be a string")
            .ThrowAsJavaScriptException();
        return info.Env().Undefined();
    }
    if (!info[2].IsBuffer()) {
        Napi::TypeError::New(info.Env(), "The 3rd argument should be a buffer")
            .ThrowAsJavaScriptException();
        return info.Env().Undefined();
    }
    if (!info[3].IsFunction()) {
        Napi::TypeError::New(info.Env(), "The 4th argument should be the callback")
            .ThrowAsJavaScriptException();
        return info.Env().Undefined();
    }

    int64_t parent_handle = decode_from_double(info[0].As<Napi::Number>().DoubleValue());
    std::string func_name(info[1].As<Napi::String>());
    Napi::Buffer<char> buffer = info[2].As<Napi::Buffer<char>>();
    std::span<const char> input(buffer.Data(), buffer.Length());

    Napi::Function cb = info[3].As<Napi::Function>();
    int64_t handle;
    if (worker_->NewOutgoingFuncCall(parent_handle, func_name, input, &handle)) {
        outgoing_func_call_cbs_[handle] = Napi::Persistent(cb);
    } else {
        cb.Call(info.Env().Global(), {
            Napi::TypeError::New(info.Env(), "NewOutgoingFuncCall failed").Value()
        });
    }
    return info.Env().Undefined();
}

Napi::Value Engine::GrpcCall(const Napi::CallbackInfo& info) {
    if (info.Length() != 5) {
        Napi::TypeError::New(info.Env(), "grpcCall takes 5 arguments")
            .ThrowAsJavaScriptException();
        return info.Env().Undefined();
    }
    if (!info[0].IsNumber()) {
        Napi::TypeError::New(info.Env(), "The 1st argument should be a number")
            .ThrowAsJavaScriptException();
        return info.Env().Undefined();
    }
    if (!info[1].IsString()) {
        Napi::TypeError::New(info.Env(), "The 2nd argument should be a string")
            .ThrowAsJavaScriptException();
        return info.Env().Undefined();
    }
    if (!info[2].IsString()) {
        Napi::TypeError::New(info.Env(), "The 3rd argument should be a string")
            .ThrowAsJavaScriptException();
        return info.Env().Undefined();
    }
    if (!info[3].IsBuffer()) {
        Napi::TypeError::New(info.Env(), "The 4th argument should be a buffer")
            .ThrowAsJavaScriptException();
        return info.Env().Undefined();
    }
    if (!info[4].IsFunction()) {
        Napi::TypeError::New(info.Env(), "The 5th argument should be the callback")
            .ThrowAsJavaScriptException();
        return info.Env().Undefined();
    }

    int64_t parent_handle = decode_from_double(info[0].As<Napi::Number>().DoubleValue());
    std::string service(info[1].As<Napi::String>());
    std::string method(info[2].As<Napi::String>());
    Napi::Buffer<char> buffer = info[3].As<Napi::Buffer<char>>();
    std::span<const char> input(buffer.Data(), buffer.Length());

    Napi::Function cb = info[4].As<Napi::Function>();
    int64_t handle;
    if (worker_->NewOutgoingGrpcCall(parent_handle, service, method, input, &handle)) {
        outgoing_func_call_cbs_[handle] = Napi::Persistent(cb);
    } else {
        cb.Call(info.Env().Global(), {
            Napi::TypeError::New(info.Env(), "NewOutgoingGrpcCall failed").Value()
        });
    }
    return info.Env().Undefined();
}

void Engine::AddWatchFdReadable(int fd) {
    uv_poll_t* uv_poll = uv_poll_pool_.Get();
    UV_DCHECK_OK(uv_poll_init(uv_loop_, uv_poll, fd));
    uv_poll->data = this;
    uv_poll_to_fds_[uv_poll] = fd;
    fd_to_uv_polls_[fd] = uv_poll;
    UV_DCHECK_OK(uv_poll_start(uv_poll, UV_READABLE, &Engine::FdEventCallback));
}

void Engine::RemoveWatchFdReadable(int fd) {
    if (fd_to_uv_polls_.count(fd) == 0) {
        LOG(ERROR) << "Unknown fd: " << fd;
        return;
    }
    RemovePoll(fd_to_uv_polls_[fd]);
}

Napi::Value Engine::IncomingFuncCallFinished(const Napi::CallbackInfo& info) {
    if (info.Length() < 2) {
        Napi::TypeError::New(info.Env(), "This callback takes at least 2 arguments")
            .ThrowAsJavaScriptException();
        return info.Env().Undefined();
    }
    if (!info[0].IsNumber()) {
        Napi::TypeError::New(info.Env(), "The first argument should be a number")
            .ThrowAsJavaScriptException();
        return info.Env().Undefined();
    }
    if (!info[1].IsBoolean()) {
        Napi::TypeError::New(info.Env(), "The second argument should be a boolean")
            .ThrowAsJavaScriptException();
        return info.Env().Undefined();
    }
    int64_t handle = decode_from_double(info[0].As<Napi::Number>().DoubleValue());
    bool success = info[1].As<Napi::Boolean>().Value();
    std::span<const char> output;
    if (success) {
        if (info.Length() >= 3 && info[2].IsBuffer()) {
            Napi::Buffer<char> buffer = info[2].As<Napi::Buffer<char>>();
            output = std::span<const char>(buffer.Data(), buffer.Length());
        }
    }
    Engine* self = reinterpret_cast<Engine*>(info.Data());
    self->worker_->OnFuncExecutionFinished(handle, success, output);
    return info.Env().Undefined();
}

void Engine::OnIncomingFuncCall(int64_t handle, std::string_view method,
                                std::span<const char> request) {
    Napi::HandleScope scope(env_);
    if (worker_->is_grpc_service()) {
        handler_.MakeCallback(env_.Global(), {
            Napi::Number::New(env_, encode_to_double(handle)),
            Napi::String::New(env_, std::string(method)),
            Napi::Buffer<char>::Copy(env_, request.data(), request.size()),
            Napi::Function::New<Engine::IncomingFuncCallFinished>(env_, "func_finished_cb", this)
        });
    } else {
        handler_.MakeCallback(env_.Global(), {
            Napi::Number::New(env_, encode_to_double(handle)),
            Napi::Buffer<char>::Copy(env_, request.data(), request.size()),
            Napi::Function::New<Engine::IncomingFuncCallFinished>(env_, "func_finished_cb", this)
        });
    }
}

void Engine::OnOutgoingFuncCallComplete(int64_t handle, bool success,
                                        std::span<const char> output) {
    Napi::HandleScope scope(env_);
    DCHECK(outgoing_func_call_cbs_.count(handle) > 0);
    Napi::FunctionReference cb = std::move(outgoing_func_call_cbs_[handle]);
    outgoing_func_call_cbs_.erase(handle);
    if (success) {
        cb.Call(env_.Global(), {
            env_.Null(),
            Napi::Buffer<char>::Copy(env_, output.data(), output.size())
        });
    } else {
        cb.Call(env_.Global(), {
            Napi::TypeError::New(env_, "invokeFunc failed").Value()
        });
    }
}

void Engine::RemovePoll(uv_poll_t* uv_poll) {
    DCHECK(uv_poll_to_fds_.count(uv_poll) > 0);
    int fd = uv_poll_to_fds_[uv_poll];
    UV_DCHECK_OK(uv_poll_stop(uv_poll));
    uv_poll_to_fds_.erase(uv_poll);
    fd_to_uv_polls_.erase(fd);
    uv_close(UV_AS_HANDLE(uv_poll), &Engine::PollCloseCallback);
}

UV_POLL_CB_FOR_CLASS(Engine, FdEvent) {
    if (status != 0) {
        LOG(ERROR) << "uv_poll failed: " << uv_strerror(status);
        if (uv_poll_to_fds_.count(handle) > 0) {
            RemovePoll(handle);
        }
        return;
    }
    if (uv_poll_to_fds_.count(handle) == 0) {
        LOG(ERROR) << "Cannot find fd for this uv_poll";
        return;
    }
    int fd = uv_poll_to_fds_[handle];
    if (events & UV_READABLE) {
        worker_->OnFdReadable(fd);
    }
}

UV_CLOSE_CB_FOR_CLASS(Engine, PollClose) {
    uv_poll_pool_.Return(reinterpret_cast<uv_poll_t*>(handle));
}

}  // namespace nodejs
}  // namespace faas
