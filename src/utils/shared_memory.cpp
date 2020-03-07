#include "utils/shared_memory.h"

#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>

namespace faas {
namespace utils {

SharedMemory::SharedMemory(absl::string_view base_path)
    : base_path_(base_path) {
    struct stat statbuf;
    CHECK(stat(base_path_.c_str(), &statbuf) == 0) << base_path_ << " does not exist";
    CHECK(S_ISDIR(statbuf.st_mode)) << base_path_ << " is not a directory";
}

SharedMemory::~SharedMemory() {
    for (const auto& region : regions_) {
        LOG(WARNING) << "Unclosed shared memory region: " << region->path();
        PCHECK(munmap(region->base(), region->size()) == 0);
    }
}

bool SharedMemory::Exists(absl::string_view path) {
    std::string full_path(absl::StrFormat("%s/%s", base_path_, path));
    return access(full_path.c_str(), F_OK) == 0;
}

SharedMemory::Region* SharedMemory::Create(absl::string_view path, size_t size) {
    std::string full_path(absl::StrFormat("%s/%s", base_path_, path));
    int fd = open(full_path.c_str(), O_CREAT|O_EXCL|O_RDWR, 0644);
    PCHECK(fd != -1) << "open failed";
    PCHECK(ftruncate(fd, size) == 0) << "ftruncate failed";
    void* ptr = mmap(0, size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    PCHECK(ptr != MAP_FAILED) << "mmap failed";
    PCHECK(close(fd) == 0) << "close failed";
    memset(ptr, 0, size);
    Region* region = new Region(this, path, reinterpret_cast<char*>(ptr), size);
    regions_.insert(std::unique_ptr<Region>(region));
    return region;
}

SharedMemory::Region* SharedMemory::OpenReadOnly(absl::string_view path) {
    std::string full_path(absl::StrFormat("%s/%s", base_path_, path));
    int fd = open(full_path.c_str(), O_RDONLY);
    PCHECK(fd != -1) << "open failed";
    struct stat statbuf;
    PCHECK(fstat(fd, &statbuf) == 0) << "fstat failed";
    size_t size = static_cast<size_t>(statbuf.st_size);
    void* ptr = mmap(0, size, PROT_READ, MAP_SHARED, fd, 0);
    PCHECK(ptr != MAP_FAILED) << "mmap failed";
    PCHECK(close(fd) == 0) << "close failed";
    Region* region = new Region(this, path, reinterpret_cast<char*>(ptr), size);
    regions_.insert(std::unique_ptr<Region>(region));
    return region;
}

void SharedMemory::Close(SharedMemory::Region* region, bool remove) {
    CHECK(regions_.contains(region));
    PCHECK(munmap(region->base(), region->size()) == 0);
    if (remove) {
        std::string full_path(absl::StrFormat("%s/%s", base_path_, region->path()));
        PCHECK(unlink(full_path.c_str()) == 0);
    }
    regions_.erase(region);
}

}  // namespace utils
}  // namespace faas
