#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "utils/appendable_buffer.h"
#include "server/connection_base.h"

namespace faas {
namespace gateway {

// FuncCallContext is owned by corresponding Connection, NOT Server
class FuncCallContext {
public:
    enum Status {
        kCreated  = 0,
        kSuccess  = 1,
        kFailed   = 2,
        kNoNode   = 3,
        kNotFound = 4
    };

    explicit FuncCallContext() {}
    ~FuncCallContext() {}

    void set_func_name(std::string_view func_name) { func_name_.assign(func_name); }
    void set_method_name(std::string_view method_name) { method_name_.assign(method_name); }
    void set_h2_stream_id(int32_t h2_stream_id) { h2_stream_id_ = h2_stream_id; }
    void set_func_call(const protocol::FuncCall& func_call) { func_call_ = func_call; }
    void append_input(std::span<const char> input) { input_.AppendData(input); }
    void append_output(std::span<const char> output) { output_.AppendData(output); }
    void set_status(Status status) { status_ = status; }

    std::string_view func_name() const { return func_name_; }
    std::string_view method_name() const { return method_name_; }
    int32_t h2_stream_id() const { return h2_stream_id_; }
    protocol::FuncCall func_call() const { return func_call_; }
    std::span<const char> input() const { return input_.to_span(); }
    std::span<const char> output() const { return output_.to_span(); }
    Status status() const { return status_; }

    void Reset() {
        status_ = kCreated;
        func_name_.clear();
        method_name_.clear();
        func_call_ = protocol::kInvalidFuncCall;
        input_.Reset();
        output_.Reset();
    }

private:
    Status status_;
    std::string func_name_;
    std::string method_name_;
    int32_t h2_stream_id_;
    protocol::FuncCall func_call_;
    utils::AppendableBuffer input_;
    utils::AppendableBuffer output_;

    DISALLOW_COPY_AND_ASSIGN(FuncCallContext);
};

}  // namespace gateway
}  // namespace faas
