#include "ipc/common.h"

#include "utils/env_variables.h"
#include "utils/fs.h"

namespace faas {
namespace ipc {

namespace {
absl::once_flag init_shared_memory_instance_once;
utils::SharedMemory* shared_memory_instance = nullptr;
}

utils::SharedMemory* GetSharedMemoryInstance() {
    absl::call_once(init_shared_memory_instance_once, [] () {
        std::string base_path(utils::GetEnvVariable("FAAS_IPC_SHM_PATH", "/dev/shm/faas_ipc"));
        if (!fs_utils::IsDirectory(base_path)) {
            if (fs_utils::Exists(base_path)) {
                PCHECK(fs_utils::Remove(base_path));
            }
            PCHECK(fs_utils::MakeDirectory(base_path));
        }
        shared_memory_instance = new utils::SharedMemory(base_path);
    });
    return shared_memory_instance;
}

}  // namespace ipc
}  // namespace faas
