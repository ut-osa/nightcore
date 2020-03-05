#pragma once

#include "base/common.h"

namespace faas {
namespace utils {

// BufferPool is NOT thread-safe
class BufferPool {
public:
    explicit BufferPool(const std::string& pool_name, size_t buffer_size)
        : pool_name_(pool_name), buffer_size_(buffer_size) {}
    ~BufferPool() {}

    void Get(uv_buf_t* buf) {
        if (available_buffers_.empty()) {
            std::unique_ptr<char[]> new_buffer(new char[buffer_size_]);
            available_buffers_.push_back(new_buffer.get());
            all_buffers_.push_back(std::move(new_buffer));
            LOG(INFO) << "BufferPool[" << pool_name_ << "]: Allocate new buffer, "
                      << "current buffer count is " << all_buffers_.size();
        }
        buf->base = available_buffers_.back();
        available_buffers_.pop_back();
        buf->len = buffer_size_;
    }

    void Return(const uv_buf_t* buf) {
        CHECK_EQ(buf->len, buffer_size_);
        available_buffers_.push_back(buf->base);
    }

private:
    std::string pool_name_;
    size_t buffer_size_;
    std::vector<char*> available_buffers_;
    std::vector<std::unique_ptr<char[]>> all_buffers_;

    DISALLOW_COPY_AND_ASSIGN(BufferPool);
};

}  // namespace utils
}  // namespace faas
