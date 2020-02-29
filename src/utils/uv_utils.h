#pragma once

#include "base/common.h"

// libuv-related helper macros

#define LIBUV_CHECK_OK(UV_CALL)                  \
    do {                                         \
        int ret = UV_CALL;                       \
        if (ret != 0) {                          \
            LOG(FATAL) << "libuv call fails: "   \
                       << uv_strerror(ret);      \
        }                                        \
    } while (0)

#define DECLARE_UV_READ_CB_FOR_CLASS(FnName)                         \
    void On##FnName(ssize_t nread, const uv_buf_t* buf);             \
    static void FnName##Callback(uv_stream_t* stream, ssize_t nread, \
                                 const uv_buf_t* buf);

#define UV_READ_CB_FOR_CLASS(ClassName, FnName)                          \
    void ClassName::FnName##Callback(uv_stream_t* stream, ssize_t nread, \
                                     const uv_buf_t* buf) {              \
        ClassName* self = reinterpret_cast<ClassName*>(stream->data);    \
        self->On##FnName(nread, buf);                                    \
    }                                                                    \
    void ClassName::On##FnName(ssize_t nread, const uv_buf_t* buf)

#define DECLARE_UV_WRITE_CB_FOR_CLASS(FnName)                   \
    void On##FnName(int status);                                \
    static void FnName##Callback(uv_write_t* req, int status);

#define UV_WRITE_CB_FOR_CLASS(ClassName, FnName)                      \
    void ClassName::FnName##Callback(uv_write_t* req, int status) {   \
        ClassName* self = reinterpret_cast<ClassName*>(req->data);    \
        self->On##FnName(status);                                     \
    }                                                                 \
    void ClassName::On##FnName(int status)

#define DECLARE_UV_ALLOC_CB_FOR_CLASS(FnName)                           \
        void On##FnName(size_t suggested_size, uv_buf_t* buf);          \
    static void FnName##Callback(uv_handle_t* handle,                   \
                                 size_t suggested_size, uv_buf_t* buf);

#define UV_ALLOC_CB_FOR_CLASS(ClassName, FnName)                             \
    void ClassName::FnName##Callback(uv_handle_t* handle,                    \
                                     size_t suggested_size, uv_buf_t* buf) { \
        ClassName* self = reinterpret_cast<ClassName*>(handle->data);        \
        self->On##FnName(suggested_size, buf);                               \
    }                                                                        \
    void ClassName::On##FnName(size_t suggested_size, uv_buf_t* buf)

#define DECLARE_UV_ASYNC_CB_FOR_CLASS(FnName)          \
  void On##FnName();                                   \
    static void FnName##Callback(uv_async_t* handle);

#define UV_ASYNC_CB_FOR_CLASS(ClassName, FnName)                       \
    void ClassName::FnName##Callback(uv_async_t* handle) {             \
        ClassName* self = reinterpret_cast<ClassName*>(handle->data);  \
        self->On##FnName();                                            \
    }                                                                  \
    void ClassName::On##FnName()

#define DECLARE_UV_CLOSE_CB_FOR_CLASS(FnName)          \
  void On##FnName();                                   \
    static void FnName##Callback(uv_handle_t* handle);

#define UV_CLOSE_CB_FOR_CLASS(ClassName, FnName)                      \
    void ClassName::FnName##Callback(uv_handle_t* handle) {           \
        ClassName* self = reinterpret_cast<ClassName*>(handle->data); \
        self->On##FnName();                                           \
    }                                                                 \
    void ClassName::On##FnName()

#define DECLARE_UV_CONNECTION_CB_FOR_CLASS(FnName)                 \
  void On##FnName();                                               \
    static void FnName##Callback(uv_stream_t* server, int status);

#define UV_CONNECTION_CB_FOR_CLASS(ClassName, FnName)                     \
    void ClassName::FnName##Callback(uv_stream_t* server, int status) {   \
        ClassName* self = reinterpret_cast<ClassName*>(server->data);     \
        self->On##FnName();                                               \
    }                                                                     \
    void ClassName::On##FnName()
