#pragma once

#include "base/common.h"

namespace faas {
namespace utils {

class CleanupWrapper {
public:
    explicit CleanupWrapper(std::function<void()> fn) : fn_(fn) {}
    ~CleanupWrapper() { fn_(); }
private:
    std::function<void()> fn_;
    DISALLOW_COPY_AND_ASSIGN(CleanupWrapper);
};

}  // namespace utils
}  // namespace faas
