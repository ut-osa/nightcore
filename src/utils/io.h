#pragma once

#include "base/common.h"

namespace faas {
namespace io_utils {

template<class T>
bool SendMessage(int fd, const T& message) {
    const char* buffer = reinterpret_cast<const char*>(&message);
    size_t pos = 0;
    while (pos < sizeof(T)) {
        ssize_t nwrite = write(fd, buffer + pos, sizeof(T) - pos);
        DCHECK(nwrite != 0) << "write() returns 0";
        if (nwrite < 0) {
            if (errno == EAGAIN || errno == EINTR) {
                continue;
            }
            return false;
        }
        pos += nwrite;
    }
    return true;
}

inline bool SendData(int fd, std::span<const char> data) {
    size_t pos = 0;
    while (pos < data.size()) {
        ssize_t nwrite = write(fd, data.data() + pos, data.size() - pos);
        DCHECK(nwrite != 0) << "write() returns 0";
        if (nwrite < 0) {
            if (errno == EAGAIN || errno == EINTR) {
                continue;
            }
            return false;
        }
        pos += nwrite;
    }
    return true;
}

template<class T>
bool RecvMessage(int fd, T* message, bool* eof) {
    char* buffer = reinterpret_cast<char*>(message);
    size_t pos = 0;
    *eof = false;
    while (pos < sizeof(T)) {
        ssize_t nread = read(fd, buffer + pos, sizeof(T) - pos);
        if (nread == 0) {
            *eof = true;
            return false;
        }
        if (nread < 0) {
            if (errno == EAGAIN || errno == EINTR) {
                continue;
            }
            return false;
        }
        pos += nread;
    }
    return true;
}

}  // namespace io_utils
}  // namespace faas
