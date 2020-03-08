#pragma once

#include "base/common.h"
#include "utils/appendable_buffer.h"

namespace faas {
namespace gateway {

class HttpConnection;

class HttpSyncRequestContext {
public:
    ~HttpSyncRequestContext() {}

    absl::string_view method() const { return method_; }
    absl::string_view path() const { return path_; }
    absl::string_view header(absl::string_view field) const {
        return headers_->contains(field) ? headers_->at(field) : "";
    }
    absl::Span<const char> body() const { return body_; }

    void SetStatus(int status) { *status_ = status; }
    void SetContentType(absl::string_view content_type) {
        *content_type_ = std::string(content_type);
    }
    void AppendToResponseBody(absl::Span<const char> data) {
        response_body_buffer_->AppendData(data);
    }

private:
    absl::string_view method_;
    absl::string_view path_;
    const absl::flat_hash_map<absl::string_view, absl::string_view>* headers_;
    absl::Span<const char> body_;

    int* status_;
    std::string* content_type_;
    utils::AppendableBuffer* response_body_buffer_;

    friend class HttpConnection;
    HttpSyncRequestContext() {}

    DISALLOW_COPY_AND_ASSIGN(HttpSyncRequestContext);
};

class HttpAsyncRequestContext {
public:
    ~HttpAsyncRequestContext() {}

    absl::string_view method() const { return method_; }
    absl::string_view path() const { return path_; }
    absl::string_view header(absl::string_view field) const {
        return headers_.contains(field) ? headers_.at(field) : "";
    }
    absl::Span<const char> body() const { return body_buffer_.to_span(); }

    void SetStatus(int status) { status_ = status; }
    void SetContentType(absl::string_view content_type) {
        content_type_ = std::string(content_type);
    }
    void AppendToResponseBody(absl::Span<const char> data) {
        response_body_buffer_.AppendData(data);
    }
    // Handler should not use HttpAsyncRequestContext any longer after calling Finish()
    bool Finish();

private:
    std::string method_;
    std::string path_;
    absl::flat_hash_map<std::string, std::string> headers_;
    utils::AppendableBuffer body_buffer_;

    int status_;
    std::string content_type_;
    utils::AppendableBuffer response_body_buffer_;

    HttpConnection* connection_;
    absl::Mutex mu_;

    friend class HttpConnection;
    HttpAsyncRequestContext() {}
    void OnConnectionClose();

    DISALLOW_COPY_AND_ASSIGN(HttpAsyncRequestContext);
};

}  // namespace gateway
}  // namespace faas
