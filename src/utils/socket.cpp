#include "utils/socket.h"

#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h> 
#include <uv.h>

namespace faas {
namespace utils {

namespace {

void FillAddressPath(struct sockaddr_un* addr, std::string_view path) {
    CHECK_LT(path.length(), sizeof(addr->sun_path));
    addr->sun_family = AF_UNIX;
    memcpy(addr->sun_path, path.data(), path.length());
    addr->sun_path[path.length()] = '\0';
}

void FillAddressPort(struct sockaddr_in* addr, std::string_view ip, int port) {
    addr->sin_family = AF_INET; 
    CHECK(uv_ip4_addr(std::string(ip).c_str(), port, addr) == 0);
}

}

int UnixDomainSocketConnect(std::string_view path) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    PCHECK(fd != -1);
    struct sockaddr_un addr;
    FillAddressPath(&addr, path);
    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
        PLOG(FATAL) << "Failed to connect to " << path;
    }
    return fd;
}

int TcpSocketBindAndListen(std::string_view ip, int port, int backlog) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    PCHECK(fd != -1);
    struct sockaddr_in addr;
    FillAddressPort(&addr, ip, port);
    if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
        PLOG(FATAL) << "Failed to bind to " << ip << ":" << port;
    }
    if (listen(fd, backlog) != 0) {
        PLOG(FATAL) << "Failed to listen";
    }
    return fd;
}

int TcpSocketConnect(std::string_view ip, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    PCHECK(fd != -1);
    struct sockaddr_in addr;
    FillAddressPort(&addr, ip, port);
    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
        PLOG(FATAL) << "Failed to connect to " << ip << ":" << port;
    }
    return fd;
}

}  // namespace utils
}  // namespace faas
