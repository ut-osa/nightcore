#pragma once

#include "base/common.h"
#include "utils/appendable_buffer.h"

namespace faas {
namespace gateway {

class Connection;

class SyncRequestContext {
public:
    ~SyncRequestContext() {}

    absl::string_view method() const { return method_; }
    absl::string_view path() const { return path_; }
    absl::string_view header(absl::string_view field) const {
        if (headers_->contains(field)) {
            return headers_->at(field);
        } else {
            return "";
        }
    }
    const char* body() const { return body_; }
    size_t body_length() const { return body_length_; }

    void SetStatus(int status) { *status_ = status; }
    void SetContentType(absl::string_view content_type) {
        *content_type_ = std::string(content_type);
    }
    void AppendDataToResponseBody(const char* data, size_t length) {
        response_body_buffer_->AppendData(data, length);
    }
    void AppendStrToResponseBody(absl::string_view str) {
        response_body_buffer_->AppendStr(str);
    }

private:
    absl::string_view method_;
    absl::string_view path_;
    const absl::flat_hash_map<absl::string_view, absl::string_view>* headers_;
    const char* body_;
    size_t body_length_;

    int* status_;
    std::string* content_type_;
    utils::AppendableBuffer* response_body_buffer_;

    friend class Connection;
    SyncRequestContext() {}

    DISALLOW_COPY_AND_ASSIGN(SyncRequestContext);
};

class AsyncRequestContext {
public:
    ~AsyncRequestContext() {}

    absl::string_view method() const { return method_; }
    absl::string_view path() const { return path_; }
    absl::string_view header(absl::string_view field) const {
        if (headers_.contains(field)) {
            return headers_.at(field);
        } else {
            return "";
        }
    }
    const char* body() const { return body_buffer_.data(); }
    size_t body_length() const { return body_buffer_.length(); }

    void SetStatus(int status) { status_ = status; }
    void SetContentType(absl::string_view content_type) {
        content_type_ = std::string(content_type);
    }
    void AppendDataToResponseBody(const char* data, size_t length) {
        response_body_buffer_.AppendData(data, length);
    }
    void AppendStrToResponseBody(absl::string_view str) {
        response_body_buffer_.AppendStr(str);
    }
    // Handler should not use AsyncRequestContext any longer after calling Finish()
    bool Finish() {
        absl::MutexLock lk(&mu_);
        if (connection_ != nullptr) {
            connection_->AsyncRequestFinish(this);
            return true;
        }
        return false;
    }

private:
    std::string method_;
    std::string path_;
    absl::flat_hash_map<std::string, std::string> headers_;
    utils::AppendableBuffer body_buffer_;

    int status_;
    std::string content_type_;
    utils::AppendableBuffer response_body_buffer_;

    Connection* connection_;
    absl::Mutex mu_;

    friend class Connection;
    AsyncRequestContext() {}
    void OnConnectionClose() {
        absl::MutexLock lk(&mu_);
        CHECK(connection_ != nullptr);
        connection_ = nullptr;
    }

    DISALLOW_COPY_AND_ASSIGN(AsyncRequestContext);
};

}  // namespace gateway
}  // namespace faas
