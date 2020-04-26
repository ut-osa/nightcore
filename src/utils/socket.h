#pragma once

#include "base/common.h"

namespace faas {
namespace utils {

int UnixDomainSocketConnect(std::string_view path);

int TcpSocketBindAndListen(std::string_view addr, int port, int backlog = 4);

int TcpSocketConnect(std::string_view addr, int port);

}  // namespace utils
}  // namespace faas
