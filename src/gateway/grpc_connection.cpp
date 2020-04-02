#include "gateway/grpc_connection.h"

#include "common/time.h"
#include "gateway/server.h"
#include "gateway/io_worker.h"

#include <absl/strings/match.h>

#define HLOG(l) LOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

#define H2_CHECK_OK(NGHTTP2_CALL)                          \
    do {                                                   \
        int ret = NGHTTP2_CALL;                            \
        LOG_IF(FATAL, ret != 0) << "nghttp2 call failed: " \
                                << nghttp2_strerror(ret);  \
    } while (0)

#define H2_MAKE_NV(NAME, VALUE) { \
    (uint8_t*)(NAME),             \
    (uint8_t*)(VALUE),            \
    sizeof(NAME) - 1,             \
    sizeof(VALUE) - 1,            \
    NGHTTP2_NV_FLAG_NONE }

namespace faas {
namespace gateway {

constexpr size_t GrpcConnection::kH2FrameHeaderByteSize;

class GrpcConnection::H2StreamContext {
public:
    H2StreamContext() {}
    ~H2StreamContext() {}

    int32_t stream_id() const { return stream_id_; }
    absl::string_view path() const { return path_; }
    absl::string_view header(absl::string_view field) const {
        return headers_.contains(field) ? headers_.at(field) : "";
    }
    absl::Span<const char> body() const { return body_buffer_.to_span(); }

    void set_path(absl::string_view value) { path_ = std::string(value); }
    void set_header(absl::string_view field, absl::string_view value) {
        headers_[std::string(field)] = std::string(value);
    }

    void Reset(int32_t stream_id) {
        stream_id_ = stream_id;
        path_.clear();
        headers_.clear();
        body_buffer_.Reset();
        response_body_buffer_.Reset();
        response_body_write_pos_ = 0;
    }

    void AppendToBody(absl::Span<const char> data) {
        body_buffer_.AppendData(data);
    }

    void AppendToResponseBody(absl::Span<const char> data) {
        response_body_buffer_.AppendData(data);
    }

    // Append a null-terminated C string. Implicit conversion from
    // `const char*` to `absl::Span<const char>` will include the last
    // '\0' character, which we usually do not want.
    void AppendToResponseBody(const char* str) {
        response_body_buffer_.AppendData(str, strlen(str));
    }

private:
    int32_t stream_id_;
    std::string path_;
    absl::flat_hash_map<std::string, std::string> headers_;
    utils::AppendableBuffer body_buffer_;
    utils::AppendableBuffer response_body_buffer_;

    size_t response_body_write_pos_;

    friend class GrpcConnection;
    DISALLOW_COPY_AND_ASSIGN(H2StreamContext);
};

GrpcConnection::GrpcConnection(Server* server, int connection_id)
    : Connection(Connection::Type::Grpc, server),
      connection_id_(connection_id), io_worker_(nullptr),
      state_(kCreated), log_header_(absl::StrFormat("GrpcConnection[%d]: ", connection_id)),
      h2_session_(nullptr), uv_write_for_mem_send_ongoing_(false) {
    nghttp2_session_callbacks* callbacks;
    H2_CHECK_OK(nghttp2_session_callbacks_new(&callbacks));
    nghttp2_session_callbacks_set_on_frame_recv_callback(
        callbacks, &GrpcConnection::H2OnFrameRecvCallback);
    nghttp2_session_callbacks_set_on_stream_close_callback(
        callbacks, &GrpcConnection::H2OnStreamCloseCallback);
    nghttp2_session_callbacks_set_on_header_callback(
        callbacks, &GrpcConnection::H2OnHeaderCallback);
    nghttp2_session_callbacks_set_on_begin_headers_callback(
        callbacks, &GrpcConnection::H2OnBeginHeadersCallback);
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(
        callbacks, &GrpcConnection::H2OnDataChunkRecvCallback);
    nghttp2_session_callbacks_set_send_data_callback(
        callbacks, &GrpcConnection::H2SendDataCallback);
    H2_CHECK_OK(nghttp2_session_server_new(&h2_session_, callbacks, this));
    nghttp2_session_callbacks_del(callbacks);
}

GrpcConnection::~GrpcConnection() {
    DCHECK(state_ == kCreated || state_ == kClosed);
    nghttp2_session_del(h2_session_);
}

uv_stream_t* GrpcConnection::InitUVHandle(uv_loop_t* uv_loop) {
    UV_DCHECK_OK(uv_tcp_init(uv_loop, &uv_tcp_handle_));
    return UV_AS_STREAM(&uv_tcp_handle_);
}

void GrpcConnection::Start(IOWorker* io_worker) {
    DCHECK(state_ == kCreated);
    DCHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
    io_worker_ = io_worker;
    uv_tcp_handle_.data = this;
    UV_DCHECK_OK(uv_read_start(UV_AS_STREAM(&uv_tcp_handle_),
                               &GrpcConnection::BufferAllocCallback,
                               &GrpcConnection::RecvDataCallback));
    state_ = kRunning;
    H2SendSettingsFrame();
}

void GrpcConnection::ScheduleClose() {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
    if (state_ == kClosing) {
        HLOG(INFO) << "Already scheduled for closing";
        return;
    }
    DCHECK(state_ == kRunning);
    closed_uv_handles_ = 0;
    total_uv_handles_ = 1;
    uv_close(UV_AS_HANDLE(&uv_tcp_handle_), &GrpcConnection::CloseCallback);
    state_ = kClosing;
}

UV_READ_CB_FOR_CLASS(GrpcConnection, RecvData) {
    if (nread > 0) {
        const uint8_t* data = reinterpret_cast<const uint8_t*>(buf->base);
        size_t length = static_cast<size_t>(nread);
        ssize_t ret = nghttp2_session_mem_recv(h2_session_, data, length);
        if (ret >= 0) {
            if (ret != length) {
                HLOG(FATAL) << "nghttp2_session_mem_recv does not consume all input data";
            }
            H2SendPendingDataIfNecessary();
        } else {
            // ret < 0
            switch (ret) {
            case NGHTTP2_ERR_CALLBACK_FAILURE:
                break;
            case NGHTTP2_ERR_BAD_CLIENT_MAGIC:
            case NGHTTP2_ERR_FLOODED:
                HLOG(WARNING) << "nghttp2 failed with error: " << nghttp2_strerror(ret)
                              << ", will close the connection";
                ScheduleClose();
                break;
            default:
                HLOG(FATAL) << "nghttp2 call returns with error: " << nghttp2_strerror(ret);
            }
        }
    } else if (nread < 0) {
        if (nread == UV_EOF || nread == UV_ECONNRESET) {
            HLOG(INFO) << "gRPC connection closed by client";
        } else {
            HLOG(WARNING) << "Read error, will close the connection: "
                          << uv_strerror(nread);
        }
        ScheduleClose();
    }
    if (buf->base != 0) {
        io_worker_->ReturnReadBuffer(buf);
    }
}

UV_WRITE_CB_FOR_CLASS(GrpcConnection, DataWritten) {
    bool req_is_for_mem_send = req == &write_req_for_mem_send_;
    if (!req_is_for_mem_send) {
        io_worker_->ReturnWriteBuffer(reinterpret_cast<char*>(req->data));
        io_worker_->ReturnWriteRequest(req);
    }
    if (status != 0) {
        HLOG(ERROR) << "Failed to write data, will close this connection: "
                    << uv_strerror(status);
        ScheduleClose();
    } else if (req_is_for_mem_send) {
        uv_write_for_mem_send_ongoing_ = false;
        H2SendPendingDataIfNecessary();
    }
}

UV_ALLOC_CB_FOR_CLASS(GrpcConnection, BufferAlloc) {
    io_worker_->NewReadBuffer(suggested_size, buf);
}

UV_CLOSE_CB_FOR_CLASS(GrpcConnection, Close) {
    DCHECK_LT(closed_uv_handles_, total_uv_handles_);
    closed_uv_handles_++;
    if (closed_uv_handles_ == total_uv_handles_) {
        state_ = kClosed;
        io_worker_->OnConnectionClose(this);
    }
}

void GrpcConnection::H2SendPendingDataIfNecessary() {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
    if (state_ != kRunning) {
        HLOG(WARNING) << "GrpcConnection is closing or has closed, will not write pending messages";
        return;
    }
    if (uv_write_for_mem_send_ongoing_) {
        return;
    }
    const uint8_t* data;
    ssize_t ret = nghttp2_session_mem_send(h2_session_, &data);
    if (ret == 0) {
        return;
    }
    if (ret < 0) {
        HLOG(FATAL) << "nghttp2_session_mem_send failed with error: "
                    << nghttp2_strerror(ret);
    }
    uv_buf_t buf = {
        .base = reinterpret_cast<char*>(const_cast<uint8_t*>(data)),
        .len = static_cast<size_t>(ret)
    };
    uv_write_t* write_req = &write_req_for_mem_send_;
    uv_write_for_mem_send_ongoing_ = true;
    UV_DCHECK_OK(uv_write(write_req, UV_AS_STREAM(&uv_tcp_handle_),
                          &buf, 1, &GrpcConnection::DataWrittenCallback));
}

void GrpcConnection::H2SendSettingsFrame() {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
    nghttp2_settings_entry iv[1] = {
        { NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, 32 }
    };
    H2_CHECK_OK(nghttp2_submit_settings(h2_session_, NGHTTP2_FLAG_NONE, iv, 1));
    H2SendPendingDataIfNecessary();
}

bool GrpcConnection::ValidateAndPopulateH2Header(H2StreamContext* context,
                                                 absl::string_view name, absl::string_view value) {
    if (absl::StartsWith(name, ":")) {
        // Reserved header
        if (name == ":scheme") {
            return value == "http";
        } else if (name == ":method") {
            return value == "POST";
        } else if (name == ":path") {
            context->set_path(value);
            return true;
        } else if (name == ":authority") {
            // :authority is ignored
            return true;
        } else {
            return false;
        }
    } else {
        // Normal header
        if (name == "content-type") {
            // return value == "application/grpc";
            return true;
        } else if (name == "user-agent") {
            // user-agent is ignored
            return true;
        } else if (name == "grpc-encoding") {
            return value == "identity";
        } else if (name == "grpc-accept-encoding") {
            // grpc-accept-encoding is ignored
            return true;
        } else if (name == "grpc-message-type") {
            // grpc-message-type is ignored
            return true;
        } else {
            HLOG(WARNING) << "Non-standard header: " << name << " = " << value;
            context->set_header(name, value);
            return true;
        }
    }
}

void GrpcConnection::OnNewGrpcRequest(H2StreamContext* context) {
    HVLOG(1) << "New request on stream with stream " << context->stream_id();
    HVLOG(1) << "Path = " << context->path();

    context->AppendToResponseBody("Hello, HTTP/2 client, your body is:\n");
    context->AppendToResponseBody(context->body());
    context->AppendToResponseBody("\n");

    nghttp2_nv hdrs[] = {
        H2_MAKE_NV(":status", "404"),
        H2_MAKE_NV("content-type", "text/plain")
    };

    nghttp2_data_provider data_provider;
    data_provider.source.ptr = context;
    data_provider.read_callback = &GrpcConnection::H2DataSourceReadCallback;
    H2_CHECK_OK(nghttp2_submit_response(h2_session_, context->stream_id(),
                                        hdrs, 2, &data_provider));
}

int GrpcConnection::H2OnFrameRecv(const nghttp2_frame* frame) {
    switch (frame->hd.type) {
    case NGHTTP2_DATA:
    case NGHTTP2_HEADERS:
        if (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) {
            int32_t stream_id = frame->hd.stream_id;
            H2StreamContext* context = reinterpret_cast<H2StreamContext*>(
                nghttp2_session_get_stream_user_data(h2_session_, stream_id));
            if (context != nullptr) {
                OnNewGrpcRequest(context);
            } else {
                HLOG(WARNING) << "Cannot find H2StreamContext for stream " << stream_id;
            }
        }
    default:
        break;
    }
    return 0;
}

int GrpcConnection::H2OnStreamClose(int32_t stream_id, uint32_t error_code) {
    H2StreamContext* context = reinterpret_cast<H2StreamContext*>(
        nghttp2_session_get_stream_user_data(h2_session_, stream_id));
    if (context == nullptr) {
        HLOG(WARNING) << "Cannot find stream context for stream " << stream_id
                      << ", will close the connection";
        ScheduleClose();
        return -1;
    }
    HVLOG(1) << "HTTP/2 stream " << stream_id << " closed";
    h2_stream_context_pool_.Return(context);
    return 0;
}

int GrpcConnection::H2OnHeader(const nghttp2_frame* frame, absl::string_view name,
                               absl::string_view value, uint8_t flags) {
    if (frame->hd.type == NGHTTP2_HEADERS && frame->headers.cat == NGHTTP2_HCAT_REQUEST) {
        int32_t stream_id = frame->hd.stream_id;
        H2StreamContext* context = reinterpret_cast<H2StreamContext*>(
            nghttp2_session_get_stream_user_data(h2_session_, stream_id));
        if (context == nullptr) {
            HLOG(WARNING) << "Cannot find stream context for stream " << stream_id
                          << ", will close the connection";
            ScheduleClose();
            return -1;
        }
        if (!ValidateAndPopulateH2Header(context, name, value)) {
            HLOG(WARNING) << "Unrecognized " << name << " header '" << value << "' for stream "
                          << stream_id << ", will close the connection";
            ScheduleClose();
            return -1;
        }

    } else {
        HLOG(WARNING) << "Unexpected HTTP/2 frame within H2OnHeader";
    }
    return 0;
}

int GrpcConnection::H2OnBeginHeaders(const nghttp2_frame* frame) {
    if (frame->hd.type == NGHTTP2_HEADERS && frame->headers.cat == NGHTTP2_HCAT_REQUEST) {
        // New HTTP/2 stream
        int32_t stream_id = frame->hd.stream_id;
        H2StreamContext* context = h2_stream_context_pool_.Get();
        context->Reset(stream_id);
        H2_CHECK_OK(nghttp2_session_set_stream_user_data(h2_session_, stream_id, context));
    } else {
        HLOG(WARNING) << "Unexpected HTTP/2 frame within H2OnBeginHeaders";
    }
    return 0;
}

int GrpcConnection::H2OnDataChunkRecv(uint8_t flags, int32_t stream_id,
                                      const uint8_t* data, size_t len) {
    H2StreamContext* context = reinterpret_cast<H2StreamContext*>(
        nghttp2_session_get_stream_user_data(h2_session_, stream_id));
    if (context == nullptr) {
        HLOG(WARNING) << "Cannot find stream context for stream " << stream_id
                      << ", will close the connection";
        ScheduleClose();
        return -1;
    }
    context->AppendToBody(absl::Span<const char>(
        reinterpret_cast<const char*>(data), len));
    return 0;
}

ssize_t GrpcConnection::H2DataSourceRead(H2StreamContext* stream_context, uint8_t* buf,
                                         size_t length, uint32_t* data_flags) {
    size_t remaining_size = stream_context->response_body_buffer_.length()
                          - stream_context->response_body_write_pos_;
    if (remaining_size == 0) {
        *data_flags |= NGHTTP2_DATA_FLAG_EOF;
        return 0;
    }
    *data_flags |= NGHTTP2_DATA_FLAG_NO_COPY;
    return std::min(remaining_size, length);
}

int GrpcConnection::H2SendData(H2StreamContext* stream_context, nghttp2_frame* frame,
                               const uint8_t* framehd, size_t length) {
    DCHECK_EQ(frame->hd.stream_id, stream_context->stream_id());
    DCHECK_LE(stream_context->response_body_write_pos_ + length,
              stream_context->response_body_buffer_.length());
    if (frame->data.padlen > 0) {
        HLOG(FATAL) << "Frame padding is not implemented yet";
    }
    const char* data = stream_context->response_body_buffer_.data()
                     + stream_context->response_body_write_pos_;
    uv_buf_t hd_buf;
    io_worker_->NewWriteBuffer(&hd_buf);
    memcpy(hd_buf.base, framehd, kH2FrameHeaderByteSize);
    uv_buf_t bufs[2] = {
        { .base = hd_buf.base, .len = kH2FrameHeaderByteSize },
        { .base = const_cast<char*>(data), .len = length }
    };
    stream_context->response_body_write_pos_ += length;
    uv_write_t* write_req = io_worker_->NewWriteRequest();
    write_req->data = hd_buf.base;
    UV_DCHECK_OK(uv_write(write_req, UV_AS_STREAM(&uv_tcp_handle_),
                          bufs, 2, &GrpcConnection::DataWrittenCallback));
    return 0;
}

int GrpcConnection::H2OnFrameRecvCallback(nghttp2_session* session, const nghttp2_frame* frame,
                                          void* user_data) {
    GrpcConnection* self = reinterpret_cast<GrpcConnection*>(user_data);
    return self->H2OnFrameRecv(frame);
}

int GrpcConnection::H2OnStreamCloseCallback(nghttp2_session* session, int32_t stream_id,
                                            uint32_t error_code, void* user_data) {
    GrpcConnection* self = reinterpret_cast<GrpcConnection*>(user_data);
    return self->H2OnStreamClose(stream_id, error_code);
}

int GrpcConnection::H2OnHeaderCallback(nghttp2_session* session, const nghttp2_frame* frame,
                                       const uint8_t* name, size_t namelen,
                                       const uint8_t* value, size_t valuelen,
                                       uint8_t flags, void* user_data) {
    GrpcConnection* self = reinterpret_cast<GrpcConnection*>(user_data);
    return self->H2OnHeader(frame, absl::string_view(reinterpret_cast<const char*>(name), namelen),
                            absl::string_view(reinterpret_cast<const char*>(value), valuelen), flags);
}

int GrpcConnection::H2OnBeginHeadersCallback(nghttp2_session* session,
                                             const nghttp2_frame* frame, void* user_data) {
    GrpcConnection* self = reinterpret_cast<GrpcConnection*>(user_data);
    return self->H2OnBeginHeaders(frame);
}

int GrpcConnection::H2OnDataChunkRecvCallback(nghttp2_session* session, uint8_t flags,
                                              int32_t stream_id, const uint8_t* data, size_t len,
                                              void* user_data) {
    GrpcConnection* self = reinterpret_cast<GrpcConnection*>(user_data);
    return self->H2OnDataChunkRecv(flags, stream_id, data, len);
}

ssize_t GrpcConnection::H2DataSourceReadCallback(nghttp2_session* session, int32_t stream_id,
                                                 uint8_t* buf, size_t length, uint32_t* data_flags,
                                                 nghttp2_data_source* source, void* user_data) {
    GrpcConnection* self = reinterpret_cast<GrpcConnection*>(user_data);
    H2StreamContext* stream_context = reinterpret_cast<H2StreamContext*>(source->ptr);
    DCHECK_EQ(stream_context->stream_id(), stream_id);
    return self->H2DataSourceRead(stream_context, buf, length, data_flags);
}

int GrpcConnection::H2SendDataCallback(nghttp2_session* session, nghttp2_frame* frame,
                                       const uint8_t* framehd, size_t length,
                                       nghttp2_data_source* source, void* user_data) {
    GrpcConnection* self = reinterpret_cast<GrpcConnection*>(user_data);
    H2StreamContext* stream_context = reinterpret_cast<H2StreamContext*>(source->ptr);
    return self->H2SendData(stream_context, frame, framehd, length);
}

}  // namespace gateway
}  // namespace faas
