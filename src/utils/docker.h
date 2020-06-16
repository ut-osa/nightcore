#pragma once

#include "base/common.h"

namespace faas {
namespace docker_utils {

// cgroup_fs root by default is /sys/fs/cgroup
void SetCgroupFsRoot(std::string_view path);

constexpr size_t kContainerIdLength = 64;
extern const std::string kInvalidContainerId;

// Get container ID of the running process
// Will return kInvalidContainerId if failed
std::string GetSelfContainerId();

bool ReadCpuAcctUsage(std::string_view container_id, int64_t* value);
bool ReadCpuAcctUsageUser(std::string_view container_id, int64_t* value);
bool ReadCpuAcctUsageSys(std::string_view container_id, int64_t* value);
bool ReadCpuAcctStat(std::string_view container_id, int32_t* user, int32_t* system);

}  // namespace docker_utils
}  // namespace faas
