#pragma once

#include "base/common.h"

// libuv-related helper macros

#define UV_CHECK_OK(UV_CALL)                             \
    do {                                                 \
        int ret = UV_CALL;                               \
        LOG_IF(FATAL, ret != 0) << "libuv call failed: " \
                                << uv_strerror(ret);     \
    } while (0)

// Assume loop->data is the event loop thread
#define CHECK_IN_EVENT_LOOP_THREAD(loop)                    \
    CHECK_EQ(base::Thread::current(),                       \
             reinterpret_cast<base::Thread*>((loop)->data))

#define DECLARE_UV_READ_CB_FOR_CLASS(FnName)                         \
    void On##FnName(ssize_t nread, const uv_buf_t* buf);             \
    static void FnName##Callback(uv_stream_t* stream, ssize_t nread, \
                                 const uv_buf_t* buf);

#define UV_READ_CB_FOR_CLASS(ClassName, FnName)                          \
    void ClassName::FnName##Callback(uv_stream_t* stream, ssize_t nread, \
                                     const uv_buf_t* buf) {              \
        CHECK_IN_EVENT_LOOP_THREAD(stream->loop);                        \
        ClassName* self = reinterpret_cast<ClassName*>(stream->data);    \
        self->On##FnName(nread, buf);                                    \
    }                                                                    \
    void ClassName::On##FnName(ssize_t nread, const uv_buf_t* buf)

#define DECLARE_UV_WRITE_CB_FOR_CLASS(FnName)                   \
    void On##FnName(uv_write_t* req, int status);               \
    static void FnName##Callback(uv_write_t* req, int status);

#define UV_WRITE_CB_FOR_CLASS(ClassName, FnName)                            \
    void ClassName::FnName##Callback(uv_write_t* req, int status) {         \
        CHECK_IN_EVENT_LOOP_THREAD(req->handle->loop);                      \
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
        CHECK_IN_EVENT_LOOP_THREAD(handle->loop);                            \
        ClassName* self = reinterpret_cast<ClassName*>(handle->data);        \
        self->On##FnName(suggested_size, buf);                               \
    }                                                                        \
    void ClassName::On##FnName(size_t suggested_size, uv_buf_t* buf)

#define DECLARE_UV_ASYNC_CB_FOR_CLASS(FnName)          \
    void On##FnName();                                 \
    static void FnName##Callback(uv_async_t* handle);

#define UV_ASYNC_CB_FOR_CLASS(ClassName, FnName)                       \
    void ClassName::FnName##Callback(uv_async_t* handle) {             \
        CHECK_IN_EVENT_LOOP_THREAD(handle->loop);                      \
        ClassName* self = reinterpret_cast<ClassName*>(handle->data);  \
        self->On##FnName();                                            \
    }                                                                  \
    void ClassName::On##FnName()

#define DECLARE_UV_CLOSE_CB_FOR_CLASS(FnName)          \
    void On##FnName();                                 \
    static void FnName##Callback(uv_handle_t* handle);

#define UV_CLOSE_CB_FOR_CLASS(ClassName, FnName)                      \
    void ClassName::FnName##Callback(uv_handle_t* handle) {           \
        CHECK_IN_EVENT_LOOP_THREAD(handle->loop);                     \
        ClassName* self = reinterpret_cast<ClassName*>(handle->data); \
        self->On##FnName();                                           \
    }                                                                 \
    void ClassName::On##FnName()

#define DECLARE_UV_CONNECTION_CB_FOR_CLASS(FnName)                 \
    void On##FnName(int status);                                   \
    static void FnName##Callback(uv_stream_t* server, int status);

#define UV_CONNECTION_CB_FOR_CLASS(ClassName, FnName)                     \
    void ClassName::FnName##Callback(uv_stream_t* server, int status) {   \
        CHECK_IN_EVENT_LOOP_THREAD(server->loop);                         \
        ClassName* self = reinterpret_cast<ClassName*>(server->data);     \
        self->On##FnName(status);                                         \
    }                                                                     \
    void ClassName::On##FnName(int status)

#define DECLARE_UV_CONNECT_CB_FOR_CLASS(FnName)                  \
    void On##FnName(int status);                                 \
    static void FnName##Callback(uv_connect_t* req, int status);

#define UV_CONNECT_CB_FOR_CLASS(ClassName, FnName)                         \
    void ClassName::FnName##Callback(uv_connect_t* req, int status) {      \
        CHECK_IN_EVENT_LOOP_THREAD(req->handle->loop);                     \
        ClassName* self = reinterpret_cast<ClassName*>(req->handle->data); \
        self->On##FnName(status);                                          \
    }                                                                      \
    void ClassName::On##FnName(int status)
