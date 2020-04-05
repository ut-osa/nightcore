#pragma once

#ifndef __FAAS_SRC
#error utils/dynamic_library.h cannot be included outside
#endif

#include "base/common.h"

#include <dlfcn.h>

namespace faas {
namespace utils {

class DynamicLibrary {
public:
    ~DynamicLibrary() {
        if (dlclose(handle_) != 0) {
            LOG(FATAL) << "Failed to close dynamic library: " << dlerror();
        }
    }

    static std::unique_ptr<DynamicLibrary> Create(std::string_view path) {
        void* handle = dlopen(std::string(path).c_str(), RTLD_LAZY);
        if (handle == nullptr) {
            LOG(FATAL) << "Failed to open dynamic library " << path << ": " << dlerror();
        }
        DynamicLibrary* dynamic_library = new DynamicLibrary(handle);
        return std::unique_ptr<DynamicLibrary>(dynamic_library);
    }

    template<class T>
    T LoadSymbol(std::string_view name) {
        void* ptr = dlsym(handle_, std::string(name).c_str());
        if (ptr == nullptr) {
            LOG(FATAL) << "Cannot load symbol " << name << " from the dynamic library";
        }
        return reinterpret_cast<T>(ptr);
    }

private:
    void* handle_;
    explicit DynamicLibrary(void* handle): handle_(handle) {}
    DISALLOW_COPY_AND_ASSIGN(DynamicLibrary);
};

}  // namespace utils
}  // namespace faas
