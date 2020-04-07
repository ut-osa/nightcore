#define __FAAS_USED_IN_BINDING
#include "utils/shared_memory.h"

#include "common/time.h"
#include "utils/fs.h"

#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace faas {
namespace utils {

SharedMemory::SharedMemory(std::string_view base_path)
    : base_path_(base_path),
      mmap_delay_stat_(
          stat::StatisticsCollector<uint32_t>::StandardReportCallback("mmap_delay")) {
    CHECK(fs_utils::IsDirectory(base_path_));
}

SharedMemory::~SharedMemory() {
    absl::MutexLock lk(&regions_mu_);
    for (const auto& item : regions_) {
        Region* region = item.first;
        LOG(WARNING) << "Unclosed shared memory region: " << region->path();
        PCHECK(munmap(region->base(), region->size()) == 0);
    }
}

SharedMemory::Region* SharedMemory::Create(std::string_view path, size_t size) {
    std::string full_path = GetFullPath(path);
    uint64_t start_timestamp = GetMonotonicMicroTimestamp();
    int fd = open(full_path.c_str(), O_CREAT|O_EXCL|O_RDWR, 0644);
    PCHECK(fd != -1) << "open failed";
    PCHECK(ftruncate(fd, size) == 0) << "ftruncate failed";
    void* ptr = nullptr;
    if (size > 0) {
        ptr = mmap(0, size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
        PCHECK(ptr != MAP_FAILED) << "mmap failed";
        PCHECK(close(fd) == 0) << "close failed";
        memset(ptr, 0, size);
        mmap_delay_stat_.AddSample(GetMonotonicMicroTimestamp() - start_timestamp);
    }
    Region* region = new Region(this, path, reinterpret_cast<char*>(ptr), size);
    AddRegion(region);
    return region;
}

SharedMemory::Region* SharedMemory::OpenReadOnly(std::string_view path) {
    std::string full_path = GetFullPath(path);
    uint64_t start_timestamp = GetMonotonicMicroTimestamp();
    int fd = open(full_path.c_str(), O_RDONLY);
    PCHECK(fd != -1) << "open failed";
    struct stat statbuf;
    PCHECK(fstat(fd, &statbuf) == 0) << "fstat failed";
    size_t size = static_cast<size_t>(statbuf.st_size);
    void* ptr = nullptr;
    if (size > 0) {
        ptr = mmap(0, size, PROT_READ, MAP_SHARED, fd, 0);
        PCHECK(ptr != MAP_FAILED) << "mmap failed";
        PCHECK(close(fd) == 0) << "close failed";
        mmap_delay_stat_.AddSample(GetMonotonicMicroTimestamp() - start_timestamp);
    }
    Region* region = new Region(this, path, reinterpret_cast<char*>(ptr), size);
    AddRegion(region);
    return region;
}

void SharedMemory::Close(SharedMemory::Region* region, bool remove) {
    absl::MutexLock lk(&regions_mu_);
    DCHECK(regions_.count(region) > 0);
    if (region->size() > 0) {
        PCHECK(munmap(region->base(), region->size()) == 0);
    }
    if (remove) {
        PCHECK(fs_utils::Remove(GetFullPath(region->path())));
    }
    regions_.erase(region);
}

void SharedMemory::AddRegion(Region* region) {
    absl::MutexLock lk(&regions_mu_);
    regions_[region] = std::unique_ptr<Region>(region);
}

std::string SharedMemory::InputPath(uint64_t full_call_id) {
    return fmt::format("{}.i", full_call_id);
}

std::string SharedMemory::OutputPath(uint64_t full_call_id) {
    return fmt::format("{}.o", full_call_id);
}

std::string SharedMemory::GetFullPath(std::string_view path) {
    return fmt::format("{}/{}", base_path_, path);
}

}  // namespace utils
}  // namespace faas
