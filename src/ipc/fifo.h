#pragma once

#include "base/common.h"

namespace faas {
namespace ipc {

void FifoCreate(std::string_view name);
void FifoRemove(std::string_view name);
int FifoOpenForRead(std::string_view name, bool nonblocking = true);
int FifoOpenForWrite(std::string_view name, bool nonblocking = true);

}  // namespace ipc
}  // namespace faas
