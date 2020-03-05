#pragma once

#include "base/common.h"

namespace faas {
namespace utils {

class AppendableBuffer {
public:
    static constexpr int kInlineBufferSize = 48;
    static constexpr int kDefaultInitialSize = kInlineBufferSize;

    explicit AppendableBuffer(int initial_size = kDefaultInitialSize)
        : buf_size_(initial_size), pos_(0) {
        if (initial_size <= kInlineBufferSize) {
            buf_ = inline_buf_;
            buf_size_ = kInlineBufferSize;
        } else {
            buf_ = reinterpret_cast<char*>(malloc(initial_size));
        }
    }

    ~AppendableBuffer() {
        if (buf_ != inline_buf_) {
            delete[] buf_;
        }
    }

    void AppendData(const char* data, int length) {
        int new_size = buf_size_;
        while (pos_ + length > new_size) {
            new_size *= 2;
        }
        if (new_size > buf_size_) {
            char* new_buf = reinterpret_cast<char*>(malloc(new_size));
            memcpy(new_buf, buf_, pos_);
            if (buf_ != inline_buf_) {
                free(buf_);
            }
            buf_ = new_buf;
            buf_size_ = new_size;
        }
        memcpy(buf_ + pos_, data, length);
        pos_ += length;
    }

    void AppendStr(const char* str) {
        AppendData(str, strlen(str));
    }

    void AppendStr(absl::string_view str) {
        AppendData(str.data(), str.length());
    }

    void Reset() { pos_ = 0; }

    const char* data() const { return buf_; }
    char* data() { return buf_; }
    size_t length() const { return pos_; }
    size_t buffer_size() const { return buf_size_; }

    void Swap(AppendableBuffer& other) {
        // In all cases, buffer size and position can be directly swapped
        std::swap(pos_, other.pos_);
        std::swap(buf_size_, other.buf_size_);
        if (buf_ != inline_buf_) {
            // Myself uses malloc buffer
            if (other.buf_ != other.inline_buf_) {
                // The other also uses malloc buffer, just swap two buffers
                std::swap(buf_, other.buf_);
            } else {
                // The other uses inline buffer
                // Set other's buffer to my malloc buffer
                other.buf_ = buf_;
                // Copy other's inline buffer data to my inline buffer
                memcpy(inline_buf_, other.inline_buf_, kInlineBufferSize);
                // Set my buffer to use inline buffer
                buf_ = inline_buf_;
            }
        } else {
            // Myself uses inline buffer
            if (other.buf_ != other.inline_buf_) {
                // The other uses malloc buffer
                buf_ = other.buf_;
                memcpy(other.inline_buf_, inline_buf_, kInlineBufferSize);
                other.buf_ = other.inline_buf_;
            } else {
                // The other also uses inline buffer, just swap contents of two inline buffers
                char tmp_buffer[kInlineBufferSize];
                memcpy(tmp_buffer, inline_buf_, kInlineBufferSize);
                memcpy(inline_buf_, other.inline_buf_, kInlineBufferSize);
                memcpy(other.inline_buf_, tmp_buffer, kInlineBufferSize);
            }
        }
    }

private:
    int buf_size_;
    int pos_;
    char* buf_;
    char inline_buf_[kInlineBufferSize];

    DISALLOW_COPY_AND_ASSIGN(AppendableBuffer);
};

}  // namespace utils
}  // namespace faas
