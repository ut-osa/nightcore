#include "utils/docker.h"

#include "utils/fs.h"

namespace faas {
namespace docker_utils {

namespace {
std::string cgroupfs_root = "/sys/fs/cgroup";
}

void SetCgroupFsRoot(std::string_view path) {
    cgroupfs_root = std::string(path);
}

const std::string kInvalidContainerId(kContainerIdLength, '0');

std::string GetSelfContainerId() {
    std::string contents;
    if (!fs_utils::ReadContents("/proc/self/cgroup", &contents)) {
        LOG(ERROR) << "Failed to read /proc/self/cgroup";
        return kInvalidContainerId;
    }
    size_t pos = contents.find("/docker/");
    if (pos == std::string::npos
          || pos + strlen("/docker/") + kContainerIdLength >= contents.length()) {
        LOG(ERROR) << "Cannot find docker's cgroup in /proc/self/cgroup";
        return kInvalidContainerId;
    }
    return contents.substr(pos + strlen("/docker/"), kContainerIdLength);
}

bool ReadCpuAcctUsage(std::string_view container_id, int64_t* value) {
    std::string full_path(fmt::format(
        "{}/cpuacct/docker/{}/cpuacct.usage", cgroupfs_root, container_id));
    std::string contents;
    if (!fs_utils::ReadContents(full_path, &contents)) {
        return false;
    }
    return absl::SimpleAtoi(contents, value);
}

bool ReadCpuAcctStat(std::string_view container_id, int32_t* user, int32_t* system) {
    std::string full_path(fmt::format(
        "{}/cpuacct/docker/{}/cpuacct.stat", cgroupfs_root, container_id));
    std::string contents;
    if (!fs_utils::ReadContents(full_path, &contents)) {
        return false;
    }
    for (const auto& line : absl::StrSplit(contents, '\n', absl::SkipWhitespace())) {
        if (absl::StartsWith(line, "user ")) {
            if (!absl::SimpleAtoi(absl::StripPrefix(line, "user "), user)) {
                return false;
            }
        }
        if (absl::StartsWith(line, "system ")) {
            if (!absl::SimpleAtoi(absl::StripPrefix(line, "system "), system)) {
                return false;
            }
        }
    }
    return true;
}

}  // namespace docker_utils
}  // namespace faas
