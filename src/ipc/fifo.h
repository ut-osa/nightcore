#pragma once

#include "base/common.h"

namespace faas {
namespace ipc {

bool FifoCreate(std::string_view name);
void FifoRemove(std::string_view name);
// FifoOpenFor{Read, Write, ReadWrite} returns -1 on failure
int FifoOpenForRead(std::string_view name, bool nonblocking = true);
int FifoOpenForWrite(std::string_view name, bool nonblocking = true);
int FifoOpenForReadWrite(std::string_view name, bool nonblocking = true);

void FifoUnsetNonblocking(int fd);
bool FifoPollForRead(int fd, int timeout_ms = -1);

}  // namespace ipc
}  // namespace faas
