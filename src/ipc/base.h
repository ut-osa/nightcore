#pragma once

#include "base/common.h"

namespace faas {
namespace ipc {

void SetRootPathForIpc(std::string_view path, bool create = false);

std::string_view GetRootPathForIpc();
std::string_view GetEngineUnixSocketPath();
std::string_view GetRootPathForShm();
std::string_view GetRootPathForFifo();

std::string GetFuncWorkerInputFifoName(uint16_t client_id);
std::string GetFuncWorkerOutputFifoName(uint16_t client_id);

std::string GetFuncCallInputShmName(uint64_t full_call_id);
std::string GetFuncCallOutputShmName(uint64_t full_call_id);
std::string GetFuncCallOutputFifoName(uint64_t full_call_id);

}  // namespace ipc
}  // namespace faas
