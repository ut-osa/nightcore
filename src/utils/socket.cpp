#include "utils/socket.h"

#include <sys/socket.h>
#include <sys/un.h>

namespace faas {
namespace utils {

namespace {

void FillAddressPath(struct sockaddr_un* addr, std::string_view path) {
    CHECK_LT(path.length(), sizeof(addr->sun_path));
    memcpy(addr->sun_path, path.data(), path.length());
    addr->sun_path[path.length()] = '\0';
}

}

int UnixDomainSocketConnect(std::string_view path) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    PCHECK(fd != -1);
    struct sockaddr_un addr;
    addr.sun_family = AF_UNIX;
    FillAddressPath(&addr, path);
    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
        PLOG(FATAL) << "Failed to connect to " << path;
    }
    return fd;
}

}  // namespace utils
}  // namespace faas
