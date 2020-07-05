#define __FAAS_USED_IN_BINDING
#include "ipc/fifo.h"

#include "ipc/base.h"
#include "utils/fs.h"
#include "common/time.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <poll.h>

namespace faas {
namespace ipc {

bool FifoCreate(std::string_view name) {
    std::string full_path = fs_utils::JoinPath(GetRootPathForFifo(), name);
    if (mkfifo(full_path.c_str(), __FAAS_FILE_CREAT_MODE) != 0) {
        PLOG(ERROR) << "mkfifo " << full_path << " failed";
        return false;
    }
    return true;
}

void FifoRemove(std::string_view name) {
    std::string full_path = fs_utils::JoinPath(GetRootPathForFifo(), name);
    if (!fs_utils::Remove(full_path)) {
        PLOG(ERROR) << "Failed to remove " << full_path;
    }
}

int FifoOpenForRead(std::string_view name, bool nonblocking) {
    std::string full_path = fs_utils::JoinPath(GetRootPathForFifo(), name);
    int flags = O_RDONLY | O_CLOEXEC;
    if (nonblocking) {
        flags |= O_NONBLOCK;
    }
    int fd = open(full_path.c_str(), flags);
    if (fd == -1) {
        PLOG(ERROR) << "open " << full_path << " failed";
    }
    return fd;
}

int FifoOpenForWrite(std::string_view name, bool nonblocking) {
    std::string full_path = fs_utils::JoinPath(GetRootPathForFifo(), name);
    int flags = O_WRONLY | O_CLOEXEC;
    if (nonblocking) {
        flags |= O_NONBLOCK;
    }
    int fd = open(full_path.c_str(), flags);
    if (fd == -1) {
        PLOG(ERROR) << "open " << full_path << " failed";
    }
    return fd;
}

int FifoOpenForReadWrite(std::string_view name, bool nonblocking) {
    std::string full_path = fs_utils::JoinPath(GetRootPathForFifo(), name);
    int flags = O_RDWR | O_CLOEXEC;
    if (nonblocking) {
        flags |= O_NONBLOCK;
    }
    int fd = open(full_path.c_str(), flags);
    if (fd == -1) {
        PLOG(ERROR) << "open " << full_path << " failed";
    }
    return fd;
}

void FifoUnsetNonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    PCHECK(flags != -1) << "fcntl F_GETFL failed";
    PCHECK(fcntl(fd, F_SETFL, flags & ~O_NONBLOCK) == 0)
        << "fcntl F_SETFL failed";
}

bool FifoPollForRead(int fd, int timeout_ms) {
    struct pollfd pfd;
    pfd.fd = fd;
    pfd.events = POLLIN;
    int ret = poll(&pfd, 1, timeout_ms);
    if (ret == -1) {
        PLOG(ERROR) << "poll failed";
        return false;
    }
    if (ret == 0) {
        LOG(ERROR) << "poll on given fifo timeout";
        return false;
    }
    if ((pfd.revents & POLLIN) == 0) {
        LOG(ERROR) << "Error happens on given fifo: revents=" << pfd.revents;
        return false;
    }
    return true;
}

}  // namespace ipc
}  // namespace faas
