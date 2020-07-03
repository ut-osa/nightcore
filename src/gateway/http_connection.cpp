#include "gateway/http_connection.h"

#include "common/time.h"
#include "gateway/server.h"

#include <nlohmann/json.hpp>
using json = nlohmann::json;

#define HLOG(l) LOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

namespace faas {
namespace gateway {

HttpConnection::HttpConnection(Server* server, int connection_id)
    : server::ConnectionBase(kTypeId), server_(server), io_worker_(nullptr), state_(kCreated),
      log_header_(fmt::format("HttpConnection[{}]: ", connection_id)) {
    http_parser_init(&http_parser_, HTTP_REQUEST);
    http_parser_.data = this;
    http_parser_settings_init(&http_parser_settings_);
    http_parser_settings_.on_message_begin = &HttpConnection::HttpParserOnMessageBeginCallback;
    http_parser_settings_.on_url = &HttpConnection::HttpParserOnUrlCallback;
    http_parser_settings_.on_header_field = &HttpConnection::HttpParserOnHeaderFieldCallback;
    http_parser_settings_.on_header_value = &HttpConnection::HttpParserOnHeaderValueCallback;
    http_parser_settings_.on_headers_complete = &HttpConnection::HttpParserOnHeadersCompleteCallback;
    http_parser_settings_.on_body = &HttpConnection::HttpParserOnBodyCallback;
    http_parser_settings_.on_message_complete = &HttpConnection::HttpParserOnMessageCompleteCallback;
}

HttpConnection::~HttpConnection() {
    DCHECK(state_ == kCreated || state_ == kClosed);
}

uv_stream_t* HttpConnection::InitUVHandle(uv_loop_t* uv_loop) {
    UV_DCHECK_OK(uv_tcp_init(uv_loop, &uv_tcp_handle_));
    return UV_AS_STREAM(&uv_tcp_handle_);
}

void HttpConnection::Start(server::IOWorker* io_worker) {
    DCHECK(state_ == kCreated);
    DCHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
    io_worker_ = io_worker;
    uv_tcp_handle_.data = this;
    response_write_req_.data = this;
    state_ = kRunning;
    StartRecvData();
}

void HttpConnection::ScheduleClose() {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
    if (state_ == kClosing) {
        HLOG(INFO) << "Already scheduled for closing";
        return;
    }
    DCHECK(state_ == kRunning);
    uv_close(UV_AS_HANDLE(&uv_tcp_handle_), &HttpConnection::CloseCallback);
    state_ = kClosing;
}

void HttpConnection::StartRecvData() {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
    if (state_ != kRunning) {
        HLOG(WARNING) << "HttpConnection is closing or has closed, will not enable read event";
        return;
    }
    UV_DCHECK_OK(uv_read_start(UV_AS_STREAM(&uv_tcp_handle_),
                               &HttpConnection::BufferAllocCallback,
                               &HttpConnection::RecvDataCallback));
}

void HttpConnection::StopRecvData() {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
    if (state_ != kRunning) {
        HLOG(WARNING) << "HttpConnection is closing or has closed, will not enable read event";
        return;
    }
    UV_DCHECK_OK(uv_read_stop(UV_AS_STREAM(&uv_tcp_handle_)));
}

UV_READ_CB_FOR_CLASS(HttpConnection, RecvData) {
    auto reclaim_worker_resource = gsl::finally([this, buf] {
        if (buf->base != 0) {
            io_worker_->ReturnReadBuffer(buf);
        }
    });
    if (nread < 0) {
        if (nread == UV_EOF || nread == UV_ECONNRESET) {
            VLOG(1) << "HttpConnection closed by client";
        } else {
            HLOG(WARNING) << "Read error, will close the connection: " << uv_strerror(nread);
        }
        ScheduleClose();
        return;
    }
    if (nread == 0) {
        HLOG(WARNING) << "nread=0, will do nothing";
        return;
    }
    const char* data = buf->base;
    size_t length = gsl::narrow_cast<size_t>(nread);
    size_t parsed = http_parser_execute(&http_parser_, &http_parser_settings_, data, length);
    if (parsed < length) {
        HLOG(WARNING) << fmt::format("HTTP parsing failed: {}, will close the connection",
                                     http_errno_name(static_cast<http_errno>(http_parser_.http_errno)));
        ScheduleClose();
    }
}

UV_WRITE_CB_FOR_CLASS(HttpConnection, DataWritten) {
    HVLOG(1) << "Successfully write response, will resume receiving new data";
    if (status == 0) {
        StartRecvData();
    } else {
        HLOG(WARNING) << "Write error, will close the connection: " << uv_strerror(status);
        ScheduleClose();
    }
}

UV_ALLOC_CB_FOR_CLASS(HttpConnection, BufferAlloc) {
    io_worker_->NewReadBuffer(suggested_size, buf);
}

UV_CLOSE_CB_FOR_CLASS(HttpConnection, Close) {
    DCHECK(state_ == kClosing);
    state_ = kClosed;
    io_worker_->OnConnectionClose(this);
}

void HttpConnection::HttpParserOnMessageBegin() {
    header_field_value_flag_ = -1;
    header_field_buffer_.Reset();
    header_value_buffer_.Reset();
    header_field_buffer_pos_ = 0;
    header_value_buffer_pos_ = 0;
    url_buffer_.Reset();
    headers_.clear();
}

void HttpConnection::HttpParserOnUrl(const char* data, size_t length) {
    url_buffer_.AppendData(data, length);
}

void HttpConnection::HttpParserOnHeaderField(const char* data, size_t length) {
    if (header_field_value_flag_ == 1) {
        HttpParserOnNewHeader();
    }
    header_field_buffer_.AppendData(data, length);
    header_field_value_flag_ = 0;
}

void HttpConnection::HttpParserOnHeaderValue(const char* data, size_t length) {
    header_value_buffer_.AppendData(data, length);
    header_field_value_flag_ = 1;
}

void HttpConnection::HttpParserOnHeadersComplete() {
    if (header_field_value_flag_ == 1) {
        HttpParserOnNewHeader();
    }
    body_buffer_.Reset();
}

void HttpConnection::HttpParserOnBody(const char* data, size_t length) {
    body_buffer_.AppendData(data, length);
}

namespace {
static bool ReadParsedUrlField(const http_parser_url* parsed_url, http_parser_url_fields field,
                               const char* url_buf, std::string_view* result) {
    if ((parsed_url->field_set & (1 << field)) == 0) {
        return false;
    } else {
        *result = std::string_view(url_buf + parsed_url->field_data[field].off,
                                   parsed_url->field_data[field].len);
        return true;
    }
}

static int to_hex(char ch) {
    if ('0' <= ch && ch <= '9') {
        return ch - '0';
    } else if ('A' <= ch && ch <= 'F') {
        return ch - 'A' + 10;
    } else if ('a' <= ch && ch <= 'f') {
        return ch - 'a' + 10;
    } else {
        return -1;
    }
}

static bool ParseQueryStringPart(std::string_view data, std::string* field, std::string* value) {
    field->clear();
    value->clear();
    std::string* current = field;
    for (size_t i = 0; i < data.size(); i++) {
        char ch = data[i];
        if (ch == '=') {
            if (current == field) {
                current = value;
                continue;
            } else {
                return false;
            }
        }
        if (('A' <= ch && ch <= 'Z') || ('a' <= ch && ch <= 'z')
              || ('0' <= ch && ch <= '9')
              || ch == '~' || ch == '-' || ch == '.' || ch == '_') {
            current->push_back(ch);
            continue;
        }
        if (ch == '+') {
            current->push_back(' ');
            continue;
        }
        if (ch == '%') {
            if (i + 2 >= data.size()) {
                return false;
            }
            int a = to_hex(data[i+1]);
            int b = to_hex(data[i+2]);
            if (a == -1 || b == -1) {
                return false;
            }
            current->push_back(gsl::narrow_cast<char>(a * 16 + b));
            i += 2;
            continue;
        }
        return false;
    }
    if (current != value) {
        return false;
    }
    return true;
}

static std::string QueryStringToJSON(std::string_view qs) {
    json data;
    std::string field;
    std::string value;
    std::vector<std::string_view> parts = absl::StrSplit(qs, '&', absl::SkipEmpty());
    for (std::string_view part : parts) {
        if (!ParseQueryStringPart(part, &field, &value)) {
            LOG(WARNING) << "Invalid query string part: " << part;
        } else {
            data[field] = value;
        }
    }
    return std::string(data.dump());
}
}

void HttpConnection::HttpParserOnMessageComplete() {
    StopRecvData();
    HVLOG(1) << "Start parsing URL: " << std::string(url_buffer_.data(), url_buffer_.length());
    http_parser_url parsed_url;
    if (http_parser_parse_url(url_buffer_.data(), url_buffer_.length(), 0, &parsed_url) != 0) {
        HLOG(WARNING) << "Failed to parse URL, will close the connection";
        ScheduleClose();
        return;
    }
    std::string_view path;
    if (!ReadParsedUrlField(&parsed_url, UF_PATH, url_buffer_.data(), &path)) {
        HLOG(WARNING) << "Parsed URL misses some fields";
        ScheduleClose();
        return;
    }
    std::string_view qs;
    if (ReadParsedUrlField(&parsed_url, UF_QUERY, url_buffer_.data(), &qs)) {
        OnNewHttpRequest(http_method_str(static_cast<http_method>(http_parser_.method)), path, qs);
    } else {
        OnNewHttpRequest(http_method_str(static_cast<http_method>(http_parser_.method)), path);
    }
    ResetHttpParser();   
}

void HttpConnection::HttpParserOnNewHeader() {
    std::string_view field(header_field_buffer_.data() + header_field_buffer_pos_,
                           header_field_buffer_.length() - header_field_buffer_pos_);
    header_field_buffer_pos_ = header_field_buffer_.length();
    std::string_view value(header_value_buffer_.data() + header_value_buffer_pos_,
                           header_value_buffer_.length() - header_value_buffer_pos_);
    header_value_buffer_pos_ = header_value_buffer_.length();
    HVLOG(1) << "Parse new HTTP header: " << field << " = " << value;
    headers_[field] = value;
}

void HttpConnection::ResetHttpParser() {
    http_parser_init(&http_parser_, HTTP_REQUEST);
}

void HttpConnection::OnNewHttpRequest(std::string_view method, std::string_view path,
                                      std::string_view qs) {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
    HVLOG(1) << "New HTTP request: " << method << " " << path;

    if (!(method == "GET" || method == "POST") || !absl::StartsWith(path, "/function/")) {
        SendHttpResponse(HttpStatus::NOT_FOUND);
        return;
    }
    std::string_view func_name = absl::StripPrefix(path, "/function/");
    auto func_entry = server_->func_config()->find_by_func_name(func_name);
    if (func_entry == nullptr || (!func_entry->allow_http_get && method == "GET")) {
        SendHttpResponse(HttpStatus::NOT_FOUND);
        return;
    }

    func_call_context_.Reset();
    func_call_context_.set_func_name(func_name);
    if (func_entry->qs_as_input) {
        if (body_buffer_.length() > 0) {
            HLOG(WARNING) << "Body not empty, but qsAsInput is set for func " << func_name;
        }
        std::string encoded_json(QueryStringToJSON(qs));
        func_call_context_.append_input(std::span<const char>(encoded_json.data(),
                                                              encoded_json.length()));
    } else {
        func_call_context_.append_input(body_buffer_.to_span());
    }
    server_->OnNewHttpFuncCall(this, &func_call_context_);
}

void HttpConnection::OnFuncCallFinished(FuncCallContext* func_call_context) {
    DCHECK(func_call_context == &func_call_context_);
    io_worker_->ScheduleFunction(
        this, absl::bind_front(&HttpConnection::OnFuncCallFinishedInternal, this));
}

void HttpConnection::SendHttpResponse(HttpStatus status, std::span<const char> body) {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
    response_header_ = fmt::format(
        "HTTP/1.1 {}\r\n"
        "Date: {}\r\n"
        "Server: {}\r\n"
        "Connection: Keep-Alive\r\n"
        "Content-Type: {}\r\n"
        "Content-Length: {}\r\n"
        "\r\n",
        GetHttpStatusString(status),
        absl::FormatTime(absl::RFC1123_full, absl::Now(), absl::UTCTimeZone()),
        kServerString,
        kResponseContentType,
        body.size()
    );
    if (body.size() > 0) {
        uv_buf_t bufs[] = {
            { .base = const_cast<char*>(response_header_.data()), .len = response_header_.length() },
            { .base = const_cast<char*>(body.data()), .len = body.size() }
        };
        UV_DCHECK_OK(uv_write(&response_write_req_, UV_AS_STREAM(&uv_tcp_handle_),
                              bufs, 2, &HttpConnection::DataWrittenCallback));
    } else {
        uv_buf_t buf = {
            .base = const_cast<char*>(response_header_.data()),
            .len = response_header_.length()
        };
        UV_DCHECK_OK(uv_write(&response_write_req_, UV_AS_STREAM(&uv_tcp_handle_),
                              &buf, 1, &HttpConnection::DataWrittenCallback));
    }
}

void HttpConnection::OnFuncCallFinishedInternal() {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
    if (state_ != kRunning) {
        HLOG(WARNING) << "HttpConnection is closing or has closed, will not send response";
        return;
    }
    switch (func_call_context_.status()) {
    case FuncCallContext::kSuccess:
        SendHttpResponse(HttpStatus::OK, func_call_context_.output());
        break;
    case FuncCallContext::kNotFound:
        SendHttpResponse(HttpStatus::NOT_FOUND);
        break;
    case FuncCallContext::kNoNode:
    case FuncCallContext::kFailed:
        SendHttpResponse(HttpStatus::INTERNAL_SERVER_ERROR);
        break;
    default:
        HLOG(ERROR) << "Invalid FuncCallContext status, will close the connection";
        ScheduleClose();
    }
}

int HttpConnection::HttpParserOnMessageBeginCallback(http_parser* http_parser) {
    HttpConnection* self = reinterpret_cast<HttpConnection*>(http_parser->data);
    self->HttpParserOnMessageBegin();
    return 0;
}

int HttpConnection::HttpParserOnUrlCallback(http_parser* http_parser, const char* data, size_t length) {
    HttpConnection* self = reinterpret_cast<HttpConnection*>(http_parser->data);
    self->HttpParserOnUrl(data, length);
    return 0;
}

int HttpConnection::HttpParserOnHeaderFieldCallback(http_parser* http_parser, const char* data, size_t length) {
    HttpConnection* self = reinterpret_cast<HttpConnection*>(http_parser->data);
    self->HttpParserOnHeaderField(data, length);
    return 0;
}

int HttpConnection::HttpParserOnHeaderValueCallback(http_parser* http_parser, const char* data, size_t length) {
    HttpConnection* self = reinterpret_cast<HttpConnection*>(http_parser->data);
    self->HttpParserOnHeaderValue(data, length);
    return 0;
}

int HttpConnection::HttpParserOnHeadersCompleteCallback(http_parser* http_parser) {
    HttpConnection* self = reinterpret_cast<HttpConnection*>(http_parser->data);
    self->HttpParserOnHeadersComplete();
    return 0;
}

int HttpConnection::HttpParserOnBodyCallback(http_parser* http_parser, const char* data, size_t length) {
    HttpConnection* self = reinterpret_cast<HttpConnection*>(http_parser->data);
    self->HttpParserOnBody(data, length);
    return 0;
}

int HttpConnection::HttpParserOnMessageCompleteCallback(http_parser* http_parser) {
    HttpConnection* self = reinterpret_cast<HttpConnection*>(http_parser->data);
    self->HttpParserOnMessageComplete();
    return 0;
}

}  // namespace gateway
}  // namespace faas
