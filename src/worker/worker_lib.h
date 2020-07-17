#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "ipc/shm_region.h"

namespace faas {
namespace worker_lib {

bool GetFuncCallInput(const protocol::Message& dispatch_func_call_message,
                      std::span<const char>* input,
                      std::unique_ptr<ipc::ShmRegion>* shm_region);

void FuncCallFinished(const protocol::FuncCall& func_call,
                      bool success, std::span<const char> output, int32_t processing_time,
                      protocol::Message* response);

// pipe_buf is supposed to have a size of at least PIPE_BUF
void FifoFuncCallFinished(const protocol::FuncCall& func_call,
                          bool success, std::span<const char> output, int32_t processing_time,
                          char* pipe_buf, protocol::Message* response);

bool PrepareNewFuncCall(const protocol::FuncCall& func_call, uint64_t parent_func_call,
                        std::span<const char> input,
                        std::unique_ptr<ipc::ShmRegion>* shm_region,
                        protocol::Message* invoke_func_message);

// pipe_buf is supposed to have a size of at least PIPE_BUF
bool FifoGetFuncCallOutput(const protocol::FuncCall& func_call,
                          int output_fifo_fd, char* pipe_buf,
                          bool* success, std::span<const char>* output,
                          std::unique_ptr<ipc::ShmRegion>* shm_region,
                          bool* pipe_buf_used);

}  // namespace worker_lib
}  // namespace faas
