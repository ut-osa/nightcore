#define __FAAS_USED_IN_BINDING
#include "ipc/base.h"

#include "utils/fs.h"

namespace faas {
namespace ipc {

namespace {
std::string root_path_for_ipc    = "NOT_SET";
std::string engine_unix_ipc_path = "NOT_SET";
std::string root_path_for_shm    = "NOT_SET";
std::string root_path_for_fifo   = "NOT_SET";

static constexpr const char* kEngineUnixSocket = "engine.sock";
static constexpr const char* kShmSubPath = "shm";
static constexpr const char* kFifoSubPath = "fifo";
}

void SetRootPathForIpc(std::string_view path, bool create) {
    root_path_for_ipc = std::string(path);
    engine_unix_ipc_path = fs_utils::JoinPath(root_path_for_ipc, kEngineUnixSocket);
    root_path_for_shm = fs_utils::JoinPath(root_path_for_ipc, kShmSubPath);
    root_path_for_fifo = fs_utils::JoinPath(root_path_for_ipc, kFifoSubPath);
    if (create) {
        if (fs_utils::IsDirectory(root_path_for_ipc)) {
            PCHECK(fs_utils::RemoveDirectoryRecursively(root_path_for_ipc));
        } else if (fs_utils::Exists(root_path_for_ipc)) {
            PCHECK(fs_utils::Remove(root_path_for_ipc));
        }
        PCHECK(fs_utils::MakeDirectory(root_path_for_ipc));
        PCHECK(fs_utils::MakeDirectory(root_path_for_shm));
        PCHECK(fs_utils::MakeDirectory(root_path_for_fifo));
    } else {
        CHECK(fs_utils::IsDirectory(root_path_for_ipc)) << root_path_for_ipc << " does not exist";
        CHECK(fs_utils::IsDirectory(root_path_for_shm)) << root_path_for_shm << " does not exist";
        CHECK(fs_utils::IsDirectory(root_path_for_fifo)) << root_path_for_fifo << " does not exist";
    }
}

std::string_view GetRootPathForIpc() {
    CHECK_NE(root_path_for_ipc, "NOT_SET");
    return root_path_for_ipc;
}

std::string_view GetEngineUnixSocketPath() {
    CHECK_NE(engine_unix_ipc_path, "NOT_SET");
    return engine_unix_ipc_path;
}

std::string_view GetRootPathForShm() {
    CHECK_NE(root_path_for_shm, "NOT_SET");
    return root_path_for_shm;
}

std::string_view GetRootPathForFifo() {
    CHECK_NE(root_path_for_fifo, "NOT_SET");
    return root_path_for_fifo;
}

std::string GetFuncWorkerInputFifoName(uint16_t client_id) {
    return fmt::format("worker_{}_input", client_id);
}

std::string GetFuncWorkerOutputFifoName(uint16_t client_id) {
    return fmt::format("worker_{}_output", client_id);
}

std::string GetFuncCallInputShmName(uint64_t full_call_id) {
    return fmt::format("{}.i", full_call_id);
}

std::string GetFuncCallOutputShmName(uint64_t full_call_id) {
    return fmt::format("{}.o", full_call_id);
}

std::string GetFuncCallOutputFifoName(uint64_t full_call_id) {
    return fmt::format("{}.o", full_call_id);
}

}  // namespace ipc
}  // namespace faas
