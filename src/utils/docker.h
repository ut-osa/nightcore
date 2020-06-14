#pragma once

#include "base/common.h"

namespace faas {
namespace docker_utils {

// cgroup_fs root by default is /sys/fs/cgroup
void SetCgroupFsRoot(std::string_view path);

constexpr size_t kContainerIdLength = 64;
extern const char* kInvalidContainerId;

// Get container ID of the running process
// Will return kInvalidContainerId if failed
std::string GetSelfContainerId();

}  // namespace docker_utils
}  // namespace faas
