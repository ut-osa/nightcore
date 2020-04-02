#pragma once

#include "base/common.h"
#include "utils/appendable_buffer.h"

namespace faas {
namespace gateway {

class GrpcConnection;

class GrpcCallContext {
public:
    ~GrpcCallContext() {}

    absl::string_view service_name() const { return service_name_; }
    absl::string_view method_name() const { return method_name_; }
    absl::Span<const char> request_body() const {
        return request_body_buffer_.to_span();
    }

    void set_http_status(int value) { http_status_ = value; }
    void set_grpc_status(int value) { grpc_status_ = value; }

    void AppendToResponseBody(absl::Span<const char> data) {
        response_body_buffer_.AppendData(data);
    }
    bool Finish();

private:
    GrpcConnection* connection_;
    int32_t h2_stream_id_;
    int http_status_;
    int grpc_status_;

    std::string service_name_;
    std::string method_name_;
    utils::AppendableBuffer request_body_buffer_;
    utils::AppendableBuffer response_body_buffer_;

    absl::Mutex mu_;

    friend class GrpcConnection;
    GrpcCallContext() : http_status_(200), grpc_status_(0) {}
    void OnStreamClose();
    DISALLOW_COPY_AND_ASSIGN(GrpcCallContext);
};

}  // namespace gateway
}  // namespace faas
