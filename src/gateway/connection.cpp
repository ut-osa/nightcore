#include "gateway/connection.h"

#include "gateway/server.h"
#include "gateway/io_worker.h"

#include <absl/time/time.h>
#include <absl/time/clock.h>

#define HLOG(l) LOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

namespace faas {
namespace gateway {

Connection::Connection(Server* server, int connection_id)
    : server_(server), connection_id_(connection_id), io_worker_(nullptr),
      state_(kReady), log_header_(absl::StrFormat("Connection[%d]: ", connection_id)) {
    http_parser_init(&http_parser_, HTTP_REQUEST);
    http_parser_.data = this;
    http_parser_settings_init(&http_parser_settings_);
    http_parser_settings_.on_message_begin = &Connection::HttpParserOnMessageBeginCallback;
    http_parser_settings_.on_url = &Connection::HttpParserOnUrlCallback;
    http_parser_settings_.on_header_field = &Connection::HttpParserOnHeaderFieldCallback;
    http_parser_settings_.on_header_value = &Connection::HttpParserOnHeaderValueCallback;
    http_parser_settings_.on_headers_complete = &Connection::HttpParserOnHeadersCompleteCallback;
    http_parser_settings_.on_body = &Connection::HttpParserOnBodyCallback;
    http_parser_settings_.on_message_complete = &Connection::HttpParserOnMessageCompleteCallback;
}

Connection::~Connection() {
    CHECK(state_ == kReady || state_ == kClosed);
}

void Connection::Start(IOWorker* io_worker) {
    CHECK(state_ == kReady);
    io_worker_ = io_worker;
    uv_tcp_handle_.data = this;
    response_write_req_.data = this;
    state_ = kRunning;
    StartRecvData();
}

void Connection::Reset(int connection_id) {
    CHECK(state_ == kClosed);
    connection_id_ = connection_id;
    log_header_ = absl::StrFormat("Connection[%d]: ", connection_id);
    ResetHttpParser();
    state_ = kReady;
}

void Connection::ScheduleClose() {
    CHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
    if (state_ == kClosing) {
        HLOG(INFO) << "Connection is already scheduled for closing";
        return;
    }
    CHECK(state_ == kRunning);
    uv_close(reinterpret_cast<uv_handle_t*>(&uv_tcp_handle_),
             &Connection::CloseCallback);
    state_ = kClosing;
}

void Connection::StartRecvData() {
    CHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
    if (state_ != kRunning) {
        HLOG(WARNING) << "Connection is closing or has closed, will not enable read event";
        return;
    }
    LIBUV_CHECK_OK(uv_read_start(reinterpret_cast<uv_stream_t*>(&uv_tcp_handle_),
                                 &Connection::BufferAllocCallback,
                                 &Connection::RecvDataCallback));
}

void Connection::StopRecvData() {
    CHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
    if (state_ != kRunning) {
        HLOG(WARNING) << "Connection is closing or has closed, will not enable read event";
        return;
    }
    LIBUV_CHECK_OK(uv_read_stop(reinterpret_cast<uv_stream_t*>(&uv_tcp_handle_)));
}

UV_READ_CB_FOR_CLASS(Connection, RecvData) {
    if (nread > 0) {
        const char* data = buf->base;
        size_t length = static_cast<size_t>(nread);
        size_t parsed = http_parser_execute(&http_parser_, &http_parser_settings_, data, length);
        if (parsed < length) {
            HLOG(WARNING) << "HTTP parsing failed: "
                          << http_errno_name(static_cast<http_errno>(http_parser_.http_errno))
                          << ",  will close the connection";
            ScheduleClose();
        }

    } else if (nread < 0) {
        if (nread == UV_EOF || nread == UV_ECONNRESET) {
            HLOG(INFO) << "Connection closed by client";
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

UV_WRITE_CB_FOR_CLASS(Connection, DataWritten) {
    HVLOG(1) << "Successfully write response, will resume receiving new data";
    if (status == 0) {
        StartRecvData();
    } else {
        HLOG(WARNING) << "Write error, will close the connection: " << uv_strerror(status);
        ScheduleClose();
    }
}

UV_ALLOC_CB_FOR_CLASS(Connection, BufferAlloc) {
    io_worker_->NewReadBuffer(suggested_size, buf);
}

UV_CLOSE_CB_FOR_CLASS(Connection, Close) {
    CHECK(state_ == kClosing);
    io_worker_->OnConnectionClose(this);
    io_worker_ = nullptr;
    state_ = kClosed;
}

void Connection::HttpParserOnMessageBegin() {
    header_field_value_flag_ = -1;
    header_field_buffer_.Reset();
    header_value_buffer_.Reset();
    header_value_buffer_pos_ = 0;
    url_buffer_.Reset();
}

void Connection::HttpParserOnUrl(const char* data, size_t length) {
    url_buffer_.AppendData(data, length);
}

void Connection::HttpParserOnHeaderField(const char* data, size_t length) {
    if (header_field_value_flag_ == 1) {
        HttpParserOnNewHeader();
    }
    header_field_buffer_.AppendData(data, length);
    header_field_value_flag_ = 0;
}

void Connection::HttpParserOnHeaderValue(const char* data, size_t length) {
    header_value_buffer_.AppendData(data, length);
    header_field_value_flag_ = 1;
}

void Connection::HttpParserOnHeadersComplete() {
    if (header_field_value_flag_ == 1) {
        HttpParserOnNewHeader();
    }
    body_buffer_.Reset();
}

void Connection::HttpParserOnBody(const char* data, size_t length) {
    body_buffer_.AppendData(data, length);
}

namespace {
static bool ReadParsedUrlField(const http_parser_url* parsed_url, http_parser_url_fields field,
                               const char* url_buf, std::string* result) {
    if ((parsed_url->field_set & (1 << field)) == 0) {
        return false;
    } else {
        result->assign(url_buf + parsed_url->field_data[field].off,
                       parsed_url->field_data[field].len);
        return true;
    }
}
}

void Connection::HttpParserOnMessageComplete() {
    StopRecvData();
    HVLOG(1) << "Start parsing URL: " << std::string(url_buffer_.data(), url_buffer_.length());
    http_parser_url parsed_url;
    if (http_parser_parse_url(url_buffer_.data(), url_buffer_.length(), 0, &parsed_url) != 0) {
        HLOG(WARNING) << "Failed to parse URL, will close the connection";
        ScheduleClose();
        return;
    }
    std::string path;
    if (!ReadParsedUrlField(&parsed_url, UF_PATH, url_buffer_.data(), &path)) {
        HLOG(WARNING) << "Parsed URL misses some fields";
        ScheduleClose();
        return;
    }
    OnNewHttpRequest(http_method_str(static_cast<http_method>(http_parser_.method)),
                     path, body_buffer_.data(), body_buffer_.length());
    ResetHttpParser();   
}

void Connection::HttpParserOnNewHeader() {
    std::string field(header_field_buffer_.data(), header_field_buffer_.length());
    header_field_buffer_.Reset();
    header_value_buffer_.AppendData("\0", 1);
    const char* value = header_value_buffer_.data() + header_value_buffer_pos_;
    header_value_buffer_pos_ = header_value_buffer_.length();
    HVLOG(1) << "Parse new HTTP header: " << field << " = " << value;
    headers_[field] = value;
}

void Connection::ResetHttpParser() {
    http_parser_init(&http_parser_, HTTP_REQUEST);
}

void Connection::OnNewHttpRequest(const std::string& method, const std::string& path,
                                  const char* body, size_t body_length) {
    HVLOG(1) << "New HTTP request: " << method << " " << path;
    response_body_buffer_.Reset();
    int status = 200;
    if (method == "GET" && path == "/hello") {
        response_body_buffer_.AppendStr("Hellxo\n");
    } else if (method == "POST" && path == "/shutdown") {
        server_->ScheduleStop();
        response_body_buffer_.AppendStr("Server is shutting down\n");
    } else {
        status = 404;
    }
    SendHttpResponse(status);
}

void Connection::SendHttpResponse(int status) {
    HVLOG(1) << "Send HTTP response with status " << status;
    static const char* CRLF = "\r\n";
    std::ostringstream header;
    header << "HTTP/1.1 " << status << " ";
    switch (status) {
    case 200:
        header << "OK";
        break;
    case 400:
        header << "Bad Request";
        break;
    case 404:
        header << "Not Found";
        break;
    case 500:
        header << "Internal Server Error";
        break;
    default:
        LOG(FATAL) << "Unsupported status code " << status;
    }
    header << CRLF;
    header << "Date: " << absl::FormatTime(absl::RFC1123_full, absl::Now(), absl::UTCTimeZone()) << CRLF;
    header << "Server: FaaS/0.1" << CRLF;
    header << "Access-Control-Allow-Origin: *" << CRLF;
    header << "Content-Type: application/x-faas" << CRLF;
    header << "Content-Length: " << response_body_buffer_.length() << CRLF;
    header << "Connection: Keep-Alive" << CRLF;
    header << CRLF;
    response_header_buffer_.Reset();
    response_header_buffer_.AppendStr(header.str());
    uv_buf_t bufs[] = {
        { .base = response_header_buffer_.data(), .len = response_header_buffer_.length() },
        { .base = response_body_buffer_.data(), .len = response_body_buffer_.length() }
    };
    LIBUV_CHECK_OK(uv_write(&response_write_req_, reinterpret_cast<uv_stream_t*>(&uv_tcp_handle_),
                            bufs, 2, &Connection::DataWrittenCallback));
}

int Connection::HttpParserOnMessageBeginCallback(http_parser* http_parser) {
    Connection* self = reinterpret_cast<Connection*>(http_parser->data);
    self->HttpParserOnMessageBegin();
    return 0;
}

int Connection::HttpParserOnUrlCallback(http_parser* http_parser, const char* data, size_t length) {
    Connection* self = reinterpret_cast<Connection*>(http_parser->data);
    self->HttpParserOnUrl(data, length);
    return 0;
}

int Connection::HttpParserOnHeaderFieldCallback(http_parser* http_parser, const char* data, size_t length) {
    Connection* self = reinterpret_cast<Connection*>(http_parser->data);
    self->HttpParserOnHeaderField(data, length);
    return 0;
}

int Connection::HttpParserOnHeaderValueCallback(http_parser* http_parser, const char* data, size_t length) {
    Connection* self = reinterpret_cast<Connection*>(http_parser->data);
    self->HttpParserOnHeaderValue(data, length);
    return 0;
}

int Connection::HttpParserOnHeadersCompleteCallback(http_parser* http_parser) {
    Connection* self = reinterpret_cast<Connection*>(http_parser->data);
    self->HttpParserOnHeadersComplete();
    return 0;
}

int Connection::HttpParserOnBodyCallback(http_parser* http_parser, const char* data, size_t length) {
    Connection* self = reinterpret_cast<Connection*>(http_parser->data);
    self->HttpParserOnBody(data, length);
    return 0;
}

int Connection::HttpParserOnMessageCompleteCallback(http_parser* http_parser) {
    Connection* self = reinterpret_cast<Connection*>(http_parser->data);
    self->HttpParserOnMessageComplete();
    return 0;
}

}  // namespace gateway
}  // namespace faas
