#define __FAAS_USED_IN_BINDING
#include "worker/worker_lib.h"

#include "ipc/fifo.h"

namespace faas {
namespace worker_lib {

using protocol::FuncCall;
using protocol::Message;
using protocol::IsDispatchFuncCallMessage;
using protocol::GetFuncCallFromMessage;
using protocol::GetInlineDataFromMessage;
using protocol::SetInlineDataInMessage;
using protocol::NewFuncCallCompleteMessage;
using protocol::NewFuncCallFailedMessage;

namespace {

bool WriteOutputToShm(const FuncCall& func_call, std::span<const char> output) {
    auto output_region = ipc::ShmCreate(
        ipc::GetFuncCallOutputShmName(func_call.full_call_id), output.size());
    if (output_region == nullptr) {
        LOG(ERROR) << "ShmCreate failed";
        return false;
    }
    if (output.size() > 0) {
        memcpy(output_region->base(), output.data(), output.size());
    }
    return true;
}

bool WriteOutputToFifo(const FuncCall& func_call,
                       bool success, std::span<const char> output,
                       char* pipe_buf) {
    VLOG(1) << "Start writing output to FIFO";
    int output_fifo = ipc::FifoOpenForWrite(
        ipc::GetFuncCallOutputFifoName(func_call.full_call_id), /* nonblocking= */ true);
    if (output_fifo == -1) {
        LOG(ERROR) << "FifoOpenForWrite failed";
        return false;
    }
    auto close_output_fifo = gsl::finally([output_fifo] {
        if (close(output_fifo) != 0) {
            PLOG(ERROR) << "close failed";
        }
    });
    int32_t header = success ? output.size() : -1;
    size_t write_size = sizeof(int32_t);
    memcpy(pipe_buf, &header, sizeof(int32_t));
    if (success) {
        if (output.size() + sizeof(int32_t) <= PIPE_BUF) {
            write_size += output.size();
            DCHECK(write_size <= PIPE_BUF);
            memcpy(pipe_buf + sizeof(uint32_t), output.data(), output.size());
        } else {
            if (!WriteOutputToShm(func_call, output)) {
                return false;
            }
        }
    }
    ssize_t nwrite = write(output_fifo, pipe_buf, write_size);
    if (nwrite < 0) {
        PLOG(ERROR) << "Failed to write to output fifo";
        return false;
    }
    if (gsl::narrow_cast<size_t>(nwrite) < write_size) {
        LOG(ERROR) << "Writing " << write_size << " bytes to output fifo is not atomic";
        return false;
    }
    return true;
}

}  // anonymous namespace

bool GetFuncCallInput(const Message& dispatch_func_call_message,
                      std::span<const char>* input,
                      std::unique_ptr<ipc::ShmRegion>* shm_region) {
    if (!IsDispatchFuncCallMessage(dispatch_func_call_message)) {
        LOG(ERROR) << "Expect DispatchFuncCall message in GetFuncCallInput";
        return false;
    }
    FuncCall func_call = GetFuncCallFromMessage(dispatch_func_call_message);
    if (dispatch_func_call_message.payload_size < 0) {
        // Input in shm
        auto input_region = ipc::ShmOpen(ipc::GetFuncCallInputShmName(func_call.full_call_id));
        if (input_region == nullptr) {
            LOG(ERROR) << "ShmOpen failed";
            return false;
        }
        *input = input_region->to_span();
        *shm_region = std::move(input_region);
    } else {
        // Input in message's inline data
        *input = GetInlineDataFromMessage(dispatch_func_call_message);
    }
    return true;
}

void FifoFuncCallFinished(const FuncCall& func_call,
                          bool success, std::span<const char> output, int32_t processing_time,
                          char* pipe_buf, Message* response) {
    if (success) {
        *response = NewFuncCallCompleteMessage(func_call, processing_time);
    } else {
        *response = NewFuncCallFailedMessage(func_call);
    }
    if (func_call.client_id == 0) {
        // FuncCall from gateway, will use message's inline data if possible
        if (success) {
            if (output.size() <= MESSAGE_INLINE_DATA_SIZE) {
                SetInlineDataInMessage(response, output);
            } else {
                if (WriteOutputToShm(func_call, output)) {
                    response->payload_size = -gsl::narrow_cast<int32_t>(output.size());
                } else {
                    *response = NewFuncCallFailedMessage(func_call);
                }
            }
        }
    } else {
        // FuncCall from other FuncWorker, will use fifo for output
        if (WriteOutputToFifo(func_call, success, output, pipe_buf)) {
            response->payload_size = gsl::narrow_cast<int32_t>(output.size());
        } else {
            *response = NewFuncCallFailedMessage(func_call);
        }
    }
}

void FuncCallFinished(const protocol::FuncCall& func_call,
                      bool success, std::span<const char> output, int32_t processing_time,
                      protocol::Message* response) {
    if (success) {
        *response = NewFuncCallCompleteMessage(func_call, processing_time);
        if (output.size() <= MESSAGE_INLINE_DATA_SIZE) {
            SetInlineDataInMessage(response, output);
        } else {
            if (WriteOutputToShm(func_call, output)) {
                response->payload_size = -gsl::narrow_cast<int32_t>(output.size());
            } else {
                *response = NewFuncCallFailedMessage(func_call);
            }
        }
    } else {
        *response = NewFuncCallFailedMessage(func_call);
    }
}

bool PrepareNewFuncCall(const FuncCall& func_call, uint64_t parent_func_call,
                        std::span<const char> input,
                        std::unique_ptr<ipc::ShmRegion>* shm_region,
                        Message* invoke_func_message) {
    *invoke_func_message = NewInvokeFuncMessage(func_call, parent_func_call);
    if (input.size() <= MESSAGE_INLINE_DATA_SIZE) {
        SetInlineDataInMessage(invoke_func_message, input);
    } else {
        // Create shm for input
        auto input_region = ipc::ShmCreate(
            ipc::GetFuncCallInputShmName(func_call.full_call_id), input.size());
        if (input_region == nullptr) {
            LOG(ERROR) << "ShmCreate failed";
            return false;
        }
        input_region->EnableRemoveOnDestruction();
        if (input.size() > 0) {
            memcpy(input_region->base(), input.data(), input.size());
        }
        *shm_region = std::move(input_region);
        invoke_func_message->payload_size = -gsl::narrow_cast<int32_t>(input.size());
    }
    return true;
}

bool FifoGetFuncCallOutput(const FuncCall& func_call,
                           int output_fifo_fd, char* pipe_buf,
                           bool* success, std::span<const char>* output,
                           std::unique_ptr<ipc::ShmRegion>* shm_region,
                           bool* pipe_buf_used) {
    ssize_t nread = read(output_fifo_fd, pipe_buf, PIPE_BUF);
    if (nread < 0) {
        PLOG(ERROR) << "Failed to read from fifo";
        return false;
    }
    if (gsl::narrow_cast<size_t>(nread) < sizeof(int32_t)) {
        LOG(ERROR) << "Cannot read header from fifo";
        return false;
    }
    int32_t header;
    memcpy(&header, pipe_buf, sizeof(int32_t));
    if (header < 0) {
        *success = false;
        *pipe_buf_used = false;
        return true;
    }
    *success = true;
    size_t output_size = gsl::narrow_cast<size_t>(header);
    if (sizeof(int32_t) + output_size <= PIPE_BUF) {
        if (gsl::narrow_cast<size_t>(nread) < sizeof(int32_t) + output_size) {
            LOG(ERROR) << "Not all fifo data is read?";
            return false;
        }
        *output = std::span<const char>(pipe_buf + sizeof(int32_t), output_size);
        *pipe_buf_used = true;
        return true;
    } else if (gsl::narrow_cast<size_t>(nread) == sizeof(int32_t)) {
        auto output_region = ipc::ShmOpen(
            ipc::GetFuncCallOutputShmName(func_call.full_call_id));
        if (output_region == nullptr) {
            LOG(ERROR) << "ShmOpen failed";
            return false;
        }
        output_region->EnableRemoveOnDestruction();
        if (output_region->size() != output_size) {
            LOG(ERROR) << "Output size mismatch";
            return false;
        }
        *output = output_region->to_span();
        *shm_region = std::move(output_region);
        *pipe_buf_used = false;
        return true;
    } else {
        LOG(ERROR) << "Invalid data read from output fifo";
        return false;
    }
}

}  // namespace worker_lib
}  // namespace faas
