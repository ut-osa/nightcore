#define __FAAS_USED_IN_BINDING
#include "ipc/shm_region.h"

#include "ipc/base.h"
#include "common/stat.h"
#include "common/time.h"
#include "utils/fs.h"

#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace faas {
namespace ipc {

std::unique_ptr<ShmRegion> ShmCreate(std::string_view name, size_t size) {
    std::string full_path = fs_utils::JoinPath(GetRootPathForShm(), name);
    int fd = open(full_path.c_str(), O_CREAT|O_EXCL|O_RDWR, __FAAS_FILE_CREAT_MODE);
    if (fd == -1) {
        PLOG(ERROR) << "open " << full_path << " failed";
        return nullptr;
    }
    PCHECK(ftruncate(fd, size) == 0) << "ftruncate failed";
    void* ptr = nullptr;
    if (size > 0) {
        ptr = mmap(0, size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
        PCHECK(ptr != MAP_FAILED) << "mmap failed";
        memset(ptr, 0, size);
    }
    PCHECK(close(fd) == 0) << "close failed";
    return std::unique_ptr<ShmRegion>(new ShmRegion(name, reinterpret_cast<char*>(ptr), size));
}

std::unique_ptr<ShmRegion> ShmOpen(std::string_view name, bool readonly) {
    std::string full_path = fs_utils::JoinPath(GetRootPathForShm(), name);
    int fd = open(full_path.c_str(), readonly ? O_RDONLY : O_RDWR);
    if (fd == -1) {
        PLOG(ERROR) << "open " << full_path << " failed";
        return nullptr;
    }
    struct stat statbuf;
    PCHECK(fstat(fd, &statbuf) == 0) << "fstat failed";
    size_t size = gsl::narrow_cast<size_t>(statbuf.st_size);
    void* ptr = nullptr;
    if (size > 0) {
        ptr = mmap(0, size, readonly ? PROT_READ : (PROT_READ|PROT_WRITE), MAP_SHARED, fd, 0);
        PCHECK(ptr != MAP_FAILED) << "mmap failed";
    }
    PCHECK(close(fd) == 0) << "close failed";
    return std::unique_ptr<ShmRegion>(new ShmRegion(name, reinterpret_cast<char*>(ptr), size));
}

ShmRegion::~ShmRegion() {
    if (size_ > 0) {
        PCHECK(munmap(base_, size_) == 0);
    }
    if (remove_on_destruction_) {
        std::string full_path = fs_utils::JoinPath(GetRootPathForShm(), name_);
        if (!fs_utils::Remove(full_path)) {
            PLOG(ERROR) << "Failed to remove " << full_path;
        }
    }
}

}  // namespace ipc
}  // namespace faas
