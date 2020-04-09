#pragma once

#include "base/common.h"
#include "common/stat.h"

namespace faas {
namespace utils {

// SharedMemory is thread-safe
class SharedMemory {
public:
    explicit SharedMemory(std::string_view base_path);
    ~SharedMemory();

    static std::string InputPath(uint64_t full_call_id);
    static std::string OutputPath(uint64_t full_call_id);

    class Region {
    public:
        std::string_view path() const { return path_; }
        char* base() { return base_; }
        const char* base() const { return base_; }
        size_t size() const { return size_; }

        std::span<const char> to_span() const {
            return std::span<const char>(base_, size_);
        }
    
        void Close(bool remove = false) {
            parent_->Close(this, remove);
        }

    private:
        friend class SharedMemory;

        SharedMemory* parent_;
        std::string path_;
        char* base_;
        size_t size_;

        Region(SharedMemory* parent, std::string_view path, char* base, size_t size)
            : parent_(parent), path_(path), base_(base), size_(size) {}

        DISALLOW_COPY_AND_ASSIGN(Region);
    };

    Region* Create(std::string_view path, size_t size);
    Region* OpenReadOnly(std::string_view path);
    void Close(Region* region, bool remove = false);

private:
    std::string base_path_;
    absl::Mutex regions_mu_;
    std::unordered_map<Region*, std::unique_ptr<Region>>
        regions_ ABSL_GUARDED_BY(regions_mu_);
    stat::StatisticsCollector<int32_t> mmap_delay_stat_;

    void AddRegion(Region* region);
    std::string GetFullPath(std::string_view path);

    DISALLOW_COPY_AND_ASSIGN(SharedMemory);
};

}  // namespace utils
}  // namespace faas
