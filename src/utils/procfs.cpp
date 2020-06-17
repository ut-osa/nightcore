#include "utils/procfs.h"

#include "common/time.h"
#include "utils/fs.h"

namespace faas {
namespace procfs_utils {

bool ReadThreadStat(int tid, ThreadStat* stat) {
    stat->timestamp = GetMonotonicNanoTimestamp();

    std::string procfs_stat_path(fmt::format("/proc/self/task/{}/stat", tid));
    std::string stat_contents;
    if (!fs_utils::ReadContents(procfs_stat_path, &stat_contents)) {
        LOG(ERROR) << "Failed to read " << procfs_stat_path;
        return false;
    }
    size_t first_parentheses_pos = stat_contents.find('(');
    size_t last_parentheses_pos = stat_contents.find_last_of(')');
    if (first_parentheses_pos == std::string::npos
          || last_parentheses_pos == std::string::npos) {
        LOG(ERROR) << "Invalid /proc/[tid]/stat contents";
        return false;
    }
    std::vector<std::string_view> parts = absl::StrSplit(
        stat_contents.substr(last_parentheses_pos + 1), ' ', absl::SkipWhitespace());
    if (parts.size() != 50) {
        LOG(ERROR) << "Invalid /proc/[tid]/stat contents";
        return false;
    }
    if (!absl::SimpleAtoi(parts[11], &stat->cpu_stat_user)) {
        return false;
    }
    if (!absl::SimpleAtoi(parts[12], &stat->cpu_stat_sys)) {
        return false;
    }

    std::string procfs_status_path(fmt::format("/proc/self/task/{}/status", tid));
    std::string status_contents;
    if (!fs_utils::ReadContents(procfs_status_path, &status_contents)) {
        LOG(ERROR) << "Failed to read " << procfs_status_path;
        return false;
    }
    stat->voluntary_ctxt_switches = -1;
    stat->nonvoluntary_ctxt_switches = -1;
    for (const auto& line : absl::StrSplit(status_contents, '\n', absl::SkipWhitespace())) {
        if (absl::StartsWith(line, "voluntary_ctxt_switches:")) {
            if (!absl::SimpleAtoi(absl::StripPrefix(line, "voluntary_ctxt_switches:"),
                                  &stat->voluntary_ctxt_switches)) {
                return false;
            }
        }
        if (absl::StartsWith(line, "nonvoluntary_ctxt_switches:")) {
            if (!absl::SimpleAtoi(absl::StripPrefix(line, "nonvoluntary_ctxt_switches:"),
                                  &stat->nonvoluntary_ctxt_switches)) {
                return false;
            }
        }
    }
    if (stat->voluntary_ctxt_switches == -1 || stat->nonvoluntary_ctxt_switches == -1) {
        LOG(ERROR) << "Invalid /proc/[tid]/status contents";
        return false;
    }

    return true;
}
    
}  // namespace procfs_utils
}  // namespace faas
