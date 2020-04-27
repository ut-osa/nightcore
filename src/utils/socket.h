#pragma once

#include "base/common.h"

namespace faas {
namespace utils {

int UnixDomainSocketConnect(std::string_view path);
int TcpSocketBindAndListen(std::string_view ip, int port, int backlog = 4);
int TcpSocketConnect(std::string_view ip, int port);
int Tcp6SocketBindAndListen(std::string_view ip, int port, int backlog = 4);
int Tcp6SocketConnect(std::string_view ip, int port);

}  // namespace utils
}  // namespace faas
