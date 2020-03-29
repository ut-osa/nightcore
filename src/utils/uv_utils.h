#pragma once

#include "base/common.h"

namespace faas {
namespace uv {

class Base {
public:
    virtual ~Base() {};
protected:
    Base() {}
private:
    DISALLOW_COPY_AND_ASSIGN(Base);
};

// Assume uv_loop->data is the event loop thread
inline bool WithinEventLoop(uv_loop_t* uv_loop) {
    return base::Thread::current() == reinterpret_cast<base::Thread*>(uv_loop->data);
}

}  // namespace uv
}  // namespace faas

#if DCHECK_IS_ON()

#define UV_DCHECK_INSTANCE_OF(ptr, ClassName)                               \
    static_assert(std::is_convertible<ClassName*, faas::uv::Base*>::value,  \
                  #ClassName " is not inherited from fass::uv::Base");      \
    do {                                                                    \
        faas::uv::Base* t_ptr = reinterpret_cast<faas::uv::Base*>(ptr);     \
        try {                                                               \
            const std::type_info& r = typeid(*t_ptr);                       \
            CHECK(r == typeid(ClassName))                                   \
                << #ptr << " does not store an instance of class "          \
                << #ClassName;                                              \
        } catch (const std::bad_typeid& e) {                                \
            LOG(FATAL) << "Failed to obtain type info of " << #ptr;         \
        }                                                                   \
    } while (0)

#else

#define UV_DCHECK_INSTANCE_OF(ptr, ClassName) \
    static_cast<void>(0)

#endif

// libuv-related helper macros

#define UV_CHECK_OK(UV_CALL)                              \
    do {                                                  \
        int ret = UV_CALL;                                \
        LOG_IF(FATAL, ret != 0) << "libuv call failed: "  \
                                << uv_strerror(ret);      \
    } while (0)

#define UV_DCHECK_OK(UV_CALL)                             \
    do {                                                  \
        int ret = UV_CALL;                                \
        DLOG_IF(FATAL, ret != 0) << "libuv call failed: " \
                                 << uv_strerror(ret);     \
    } while (0)

#define UV_AS_HANDLE(uv_ptr) reinterpret_cast<uv_handle_t*>(uv_ptr)
#define UV_AS_STREAM(uv_ptr) reinterpret_cast<uv_stream_t*>(uv_ptr)

#define DCHECK_IN_EVENT_LOOP_THREAD(loop) DCHECK(uv::WithinEventLoop(loop))

#define DECLARE_UV_READ_CB_FOR_CLASS(FnName)                         \
    void On##FnName(ssize_t nread, const uv_buf_t* buf);             \
    static void FnName##Callback(uv_stream_t* stream, ssize_t nread, \
                                 const uv_buf_t* buf);

#define UV_READ_CB_FOR_CLASS(ClassName, FnName)                          \
    void ClassName::FnName##Callback(uv_stream_t* stream, ssize_t nread, \
                                     const uv_buf_t* buf) {              \
        DCHECK_IN_EVENT_LOOP_THREAD(stream->loop);                       \
        UV_DCHECK_INSTANCE_OF(stream->data, ClassName);                  \
        ClassName* self = reinterpret_cast<ClassName*>(stream->data);    \
        self->On##FnName(nread, buf);                                    \
    }                                                                    \
    void ClassName::On##FnName(ssize_t nread, const uv_buf_t* buf)

#define DECLARE_UV_WRITE_CB_FOR_CLASS(FnName)                   \
    void On##FnName(uv_write_t* req, int status);               \
    static void FnName##Callback(uv_write_t* req, int status);

#define UV_WRITE_CB_FOR_CLASS(ClassName, FnName)                            \
    void ClassName::FnName##Callback(uv_write_t* req, int status) {         \
        DCHECK_IN_EVENT_LOOP_THREAD(req->handle->loop);                     \
        UV_DCHECK_INSTANCE_OF(req->handle->data, ClassName);                \
        ClassName* self = reinterpret_cast<ClassName*>(req->handle->data);  \
        self->On##FnName(req, status);                                      \
    }                                                                       \
    void ClassName::On##FnName(uv_write_t* req, int status)

#define DECLARE_UV_ALLOC_CB_FOR_CLASS(FnName)                           \
    void On##FnName(size_t suggested_size, uv_buf_t* buf);              \
    static void FnName##Callback(uv_handle_t* handle,                   \
                                 size_t suggested_size, uv_buf_t* buf);

#define UV_ALLOC_CB_FOR_CLASS(ClassName, FnName)                             \
    void ClassName::FnName##Callback(uv_handle_t* handle,                    \
                                     size_t suggested_size, uv_buf_t* buf) { \
        DCHECK_IN_EVENT_LOOP_THREAD(handle->loop);                           \
        UV_DCHECK_INSTANCE_OF(handle->data, ClassName);                      \
        ClassName* self = reinterpret_cast<ClassName*>(handle->data);        \
        self->On##FnName(suggested_size, buf);                               \
    }                                                                        \
    void ClassName::On##FnName(size_t suggested_size, uv_buf_t* buf)

#define DECLARE_UV_ASYNC_CB_FOR_CLASS(FnName)          \
    void On##FnName();                                 \
    static void FnName##Callback(uv_async_t* handle);

#define UV_ASYNC_CB_FOR_CLASS(ClassName, FnName)                       \
    void ClassName::FnName##Callback(uv_async_t* handle) {             \
        DCHECK_IN_EVENT_LOOP_THREAD(handle->loop);                     \
        UV_DCHECK_INSTANCE_OF(handle->data, ClassName);                \
        ClassName* self = reinterpret_cast<ClassName*>(handle->data);  \
        self->On##FnName();                                            \
    }                                                                  \
    void ClassName::On##FnName()

#define DECLARE_UV_CLOSE_CB_FOR_CLASS(FnName)          \
    void On##FnName();                                 \
    static void FnName##Callback(uv_handle_t* handle);

#define UV_CLOSE_CB_FOR_CLASS(ClassName, FnName)                      \
    void ClassName::FnName##Callback(uv_handle_t* handle) {           \
        DCHECK_IN_EVENT_LOOP_THREAD(handle->loop);                    \
        UV_DCHECK_INSTANCE_OF(handle->data, ClassName);               \
        ClassName* self = reinterpret_cast<ClassName*>(handle->data); \
        self->On##FnName();                                           \
    }                                                                 \
    void ClassName::On##FnName()

#define DECLARE_UV_CONNECTION_CB_FOR_CLASS(FnName)                 \
    void On##FnName(int status);                                   \
    static void FnName##Callback(uv_stream_t* server, int status);

#define UV_CONNECTION_CB_FOR_CLASS(ClassName, FnName)                     \
    void ClassName::FnName##Callback(uv_stream_t* server, int status) {   \
        DCHECK_IN_EVENT_LOOP_THREAD(server->loop);                        \
        UV_DCHECK_INSTANCE_OF(server->data, ClassName);                   \
        ClassName* self = reinterpret_cast<ClassName*>(server->data);     \
        self->On##FnName(status);                                         \
    }                                                                     \
    void ClassName::On##FnName(int status)

#define DECLARE_UV_CONNECT_CB_FOR_CLASS(FnName)                  \
    void On##FnName(int status);                                 \
    static void FnName##Callback(uv_connect_t* req, int status);

#define UV_CONNECT_CB_FOR_CLASS(ClassName, FnName)                         \
    void ClassName::FnName##Callback(uv_connect_t* req, int status) {      \
        DCHECK_IN_EVENT_LOOP_THREAD(req->handle->loop);                    \
        UV_DCHECK_INSTANCE_OF(req->handle->data, ClassName);               \
        ClassName* self = reinterpret_cast<ClassName*>(req->handle->data); \
        self->On##FnName(status);                                          \
    }                                                                      \
    void ClassName::On##FnName(int status)

#define DECLARE_UV_EXIT_CB_FOR_CLASS(FnName)                             \
    void On##FnName(int64_t exit_status, int term_signal);               \
    static void FnName##Callback(uv_process_t* process,                  \
                                 int64_t exit_status, int term_signal);

#define UV_EXIT_CB_FOR_CLASS(ClassName, FnName)                               \
    void ClassName::FnName##Callback(uv_process_t* process,                   \
                                     int64_t exit_status, int term_signal) {  \
        DCHECK_IN_EVENT_LOOP_THREAD(process->loop);                           \
        UV_DCHECK_INSTANCE_OF(process->data, ClassName);                      \
        ClassName* self = reinterpret_cast<ClassName*>(process->data);        \
        self->On##FnName(exit_status, term_signal);                           \
    }                                                                         \
    void ClassName::On##FnName(int64_t exit_status, int term_signal)
