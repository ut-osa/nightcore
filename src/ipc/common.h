#pragma once

#ifndef __FAAS_SRC
#error ipc/common.h cannot be included outside
#endif

#include "base/common.h"
#include "utils/shared_memory.h"

namespace faas {
namespace ipc {

utils::SharedMemory* GetSharedMemoryInstance();

}  // namespace ipc
}  // namespace faas
