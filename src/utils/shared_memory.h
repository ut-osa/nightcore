#pragma once

#include "base/common.h"

namespace faas {
namespace utils {

// SharedMemory is thread-safe
class SharedMemory {
public:
    explicit SharedMemory(absl::string_view base_path);
    ~SharedMemory();

    class Region {
    public:
        absl::string_view path() const { return path_; }
        char* base() const { return base_; }
        size_t size() const { return size_; }
        void Close(bool remove = false) {
            parent_->Close(this, remove);
        }

    private:
        friend class SharedMemory;

        SharedMemory* parent_;
        std::string path_;
        char* base_;
        size_t size_;

        Region(SharedMemory* parent, absl::string_view path, char* base, size_t size)
            : parent_(parent), path_(path), base_(base), size_(size) {}

        DISALLOW_COPY_AND_ASSIGN(Region);
    };

    bool Exists(absl::string_view path);
    Region* Create(absl::string_view path, size_t size);
    Region* OpenReadOnly(absl::string_view path);
    void Close(Region* region, bool remove = false);

private:
    std::string base_path_;
    absl::Mutex regions_mu_;
    absl::flat_hash_set<std::unique_ptr<Region>> regions_ ABSL_GUARDED_BY(regions_mu_);

    DISALLOW_COPY_AND_ASSIGN(SharedMemory);
};

}  // namespace utils
}  // namespace faas
