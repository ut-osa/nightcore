#include "utils/perf_event.h"

#include <sys/ioctl.h>
#include <asm/unistd.h>

namespace faas {
namespace utils {

namespace {
long perf_event_open(struct perf_event_attr* hw_event, pid_t pid, int cpu,
                     int group_fd, unsigned long flags) {
    return syscall(__NR_perf_event_open, hw_event, pid, cpu, group_fd, flags);
}
}

PerfEventGroup::PerfEventGroup()
    : cpu_(-1), exclude_user_(false), exclude_kernel_(false), group_fd_(-1) {}

PerfEventGroup::~PerfEventGroup() {
    for (int fd : event_fds_) {
        PCHECK(close(fd) == 0);
    }
}

bool PerfEventGroup::AddEvent(uint32_t type, uint64_t config) {
    struct perf_event_attr pe;
    memset(&pe, 0, sizeof(pe));
    pe.type = type;
    pe.size = sizeof(pe);
    pe.config = config;
    pe.read_format = PERF_FORMAT_GROUP;
    pe.disabled = 1;
    pe.exclude_kernel = exclude_kernel_;
    pe.exclude_user = exclude_user_;
    int fd = perf_event_open(&pe, 0, cpu_, group_fd_, 0);
    if (fd == -1) {
        return false;
    }
    if (group_fd_ == -1) {
        group_fd_ = fd;
    }
    event_fds_.push_back(fd);
    return true;
}

void PerfEventGroup::Reset() {
    CHECK(group_fd_ != -1) << "No event has been added yet";
    for (int fd : event_fds_) {
        PCHECK(ioctl(fd, PERF_EVENT_IOC_RESET, 0) == 0) << "ioctl (reset) failed";
    }
}

void PerfEventGroup::Enable() {
    CHECK(group_fd_ != -1) << "No event has been added yet";
    for (int fd : event_fds_) {
        PCHECK(ioctl(fd, PERF_EVENT_IOC_ENABLE, 0) == 0) << "ioctl (reset) failed";
    }
}

void PerfEventGroup::Disable() {
    CHECK(group_fd_ != -1) << "No event has been added yet";
    for (int fd : event_fds_) {
        PCHECK(ioctl(fd, PERF_EVENT_IOC_DISABLE, 0) == 0) << "ioctl (reset) failed";
    }
}

std::vector<uint64_t> PerfEventGroup::ReadValues() {
    CHECK(group_fd_ != -1) << "No event has been added yet";
    uint64_t* values = new uint64_t[event_fds_.size() + 1];
    ssize_t read_size = sizeof(uint64_t) * (event_fds_.size() + 1);
    PCHECK(read(group_fd_, values, read_size) == read_size);
    CHECK_EQ(static_cast<size_t>(values[0]), event_fds_.size());
    std::vector<uint64_t> ret;
    for (size_t i = 0; i < event_fds_.size(); i++) {
        ret.push_back(values[1 + i]);
    }
    delete[] values;
    return ret;
}

}  // namespace utils
}  // namespace faas
