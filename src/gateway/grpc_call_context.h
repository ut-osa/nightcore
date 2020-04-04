#pragma once

#include "base/common.h"
#include "common/http_status.h"
#include "utils/appendable_buffer.h"

namespace faas {
namespace gateway {

class GrpcConnection;

enum class GrpcStatus {
    OK            = 0,
    CANCELLED     = 1,
    UNKNOWN       = 2,
    NOT_FOUND     = 5,
    UNIMPLEMENTED = 12
};

class GrpcCallContext {
public:
    ~GrpcCallContext() {}

    std::string_view service_name() const { return service_name_; }
    std::string_view method_name() const { return method_name_; }
    std::span<const char> request_body() const {
        return request_body_buffer_.to_span();
    }

    void set_http_status(HttpStatus value) { http_status_ = value; }
    void set_grpc_status(GrpcStatus value) { grpc_status_ = value; }

    void AppendToResponseBody(std::span<const char> data) {
        response_body_buffer_.AppendData(data);
    }
    bool Finish();

private:
    GrpcConnection* connection_;
    int32_t h2_stream_id_;
    HttpStatus http_status_;
    GrpcStatus grpc_status_;

    std::string service_name_;
    std::string method_name_;
    utils::AppendableBuffer request_body_buffer_;
    utils::AppendableBuffer response_body_buffer_;

    absl::Mutex mu_;

    friend class GrpcConnection;
    GrpcCallContext()
        : http_status_(HttpStatus::OK), grpc_status_(GrpcStatus::OK) {}
    void OnStreamClose();
    DISALLOW_COPY_AND_ASSIGN(GrpcCallContext);
};

}  // namespace gateway
}  // namespace faas
