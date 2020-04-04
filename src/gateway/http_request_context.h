#pragma once

#include "base/common.h"
#include "utils/appendable_buffer.h"

namespace faas {
namespace gateway {

class HttpConnection;

class HttpSyncRequestContext {
public:
    ~HttpSyncRequestContext() {}

    std::string_view method() const { return method_; }
    std::string_view path() const { return path_; }
    std::string_view header(std::string_view field) const {
        return headers_->contains(field) ? headers_->at(field) : "";
    }
    gsl::span<const char> body() const { return body_; }

    void SetStatus(int status) { *status_ = status; }
    void SetContentType(std::string_view content_type) {
        *content_type_ = std::string(content_type);
    }
    void AppendToResponseBody(gsl::span<const char> data) {
        response_body_buffer_->AppendData(data);
    }
    // Append a null-terminated C string. Implicit conversion from
    // `const char*` to `gsl::span<const char>` will include the last
    // '\0' character, which we usually do not want.
    void AppendToResponseBody(const char* str) {
        response_body_buffer_->AppendData(str, strlen(str));
    }

private:
    std::string_view method_;
    std::string_view path_;
    const absl::flat_hash_map<std::string_view, std::string_view>* headers_;
    gsl::span<const char> body_;

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

    std::string_view method() const { return method_; }
    std::string_view path() const { return path_; }
    std::string_view header(std::string_view field) const {
        return headers_.contains(field) ? headers_.at(field) : "";
    }
    gsl::span<const char> body() const { return body_buffer_.to_span(); }

    void SetStatus(int status) { status_ = status; }
    void SetContentType(std::string_view content_type) {
        content_type_ = std::string(content_type);
    }
    void AppendToResponseBody(gsl::span<const char> data) {
        response_body_buffer_.AppendData(data);
    }
    // Append a null-terminated C string. Implicit conversion from
    // `const char*` to `gsl::span<const char>` will include the last
    // '\0' character, which we usually do not want.
    void AppendToResponseBody(const char* str) {
        response_body_buffer_.AppendData(str, strlen(str));
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
