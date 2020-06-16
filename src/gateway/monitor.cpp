#include "gateway/monitor.h"

#include "gateway/server.h"
#include "gateway/message_connection.h"
#include "utils/docker.h"
#include "common/time.h"

#include <sys/timerfd.h>

#define HLOG(l) LOG(l) << "Monitor: "
#define HVLOG(l) VLOG(l) << "Monitor: "

namespace faas {
namespace gateway {

Monitor::Monitor(Server* server)
    : state_(kCreated), server_(server), frequency_hz_(kDefaultFrequencyHz),
      background_thread_("Monitor", absl::bind_front(&Monitor::BackgroundThreadMain, this)),
      self_container_id_(docker_utils::GetSelfContainerId()) {}

Monitor::~Monitor() {
    State state = state_.load();
    DCHECK(state == kCreated || state == kStopped);
}

void Monitor::set_frequency(float hz) {
    DCHECK(state_.load() == kCreated);
    frequency_hz_ = hz;
}

void Monitor::Start() {
    background_thread_.Start();
}

void Monitor::ScheduleStop() {
    if (state_.load() == kRunning) {
        state_.store(kStopping);
    }
}

void Monitor::WaitForFinish() {
    if (state_.load() == kStopping) {
        background_thread_.Join();
    }
}

void Monitor::OnLauncherConnected(MessageConnection* launcher_connection,
                                  std::string_view container_id) {
    uint16_t func_id = launcher_connection->func_id();
    absl::MutexLock lk(&mu_);
    if (func_container_ids_.contains(func_id)) {
        HLOG(ERROR) << fmt::format("func_id {} already exists", func_id);
        return;
    }
    HLOG(INFO) << fmt::format("New FuncContainer[{}]: container_id={}", func_id, container_id);
    func_container_ids_[func_id] = std::string(container_id);
}

namespace {
static float compute_load(int64_t timestamp1, int64_t usage1, int64_t timestamp2, int64_t usage2) {
    return gsl::narrow_cast<float>(usage2 - usage1) / gsl::narrow_cast<float>(timestamp2 - timestamp1);
}

// One tick is 10ms
static int64_t tick_to_ns(int32_t tick) {
    return int64_t{tick} * 10000000;
}
}

void Monitor::BackgroundThreadMain() {
    state_.store(kRunning);
    HLOG(INFO) << "Background thread starts";

    int64_t interval_in_ns = gsl::narrow_cast<int64_t>(float{1e9} / frequency_hz_);

    struct timespec now;
    PCHECK(clock_gettime(CLOCK_MONOTONIC, &now) == 0) << "clock_gettime failed";

    struct itimerspec timer_spec;
    timer_spec.it_value = now;
    timer_spec.it_interval.tv_sec = gsl::narrow_cast<time_t>(interval_in_ns / 1000000000);
    timer_spec.it_interval.tv_nsec = gsl::narrow_cast<long>(interval_in_ns % 1000000000);

    int timer_fd = timerfd_create(CLOCK_MONOTONIC, 0);
    PCHECK(timer_fd != -1) << "timerfd_create failed";
    PCHECK(timerfd_settime(timer_fd, TFD_TIMER_ABSTIME, &timer_spec, 0) == 0)
        << "timerfd_settime failed";

    absl::flat_hash_map</* container_id */ std::string, ContainerStat> container_stats_;

    while (true) {
        uint64_t exp;
        ssize_t nread = read(timer_fd, &exp, sizeof(uint64_t));
        if (nread < 0) {
            PLOG(ERROR) << "read on timerfd failed";
            break;
        }
        if (nread != sizeof(uint64_t)) {
            HLOG(WARNING) << "read on timerfd returns wrong size";
        } else if (exp > 1) {
            HLOG(WARNING) << "timerfd expires more than once";
        }

        std::vector<std::pair</* func_id */ int, /* container_id */ std::string>> container_ids;
        if (self_container_id_ != docker_utils::kInvalidContainerId) {
            container_ids.push_back(std::make_pair(-1, self_container_id_));
        }
        {
            absl::MutexLock lk(&mu_);
            for (const auto& entry : func_container_ids_) {
                if (entry.second != docker_utils::kInvalidContainerId) {
                    container_ids.push_back(std::make_pair(int{entry.first}, entry.second));
                }
            }
        }

        float total_load_usage = 0;
        float total_user_load_stat = 0;
        float total_sys_load_stat = 0;
        for (const auto& entry : container_ids) {
            const std::string& container_id = entry.second;
            ContainerStat stat;
            if (!ReadContainerStat(container_id, &stat)) {
                HLOG(ERROR) << "Failed to read container stat: container_id=" << container_id;
                continue;
            }
            if (!container_stats_.contains(container_id)) {
                container_stats_[container_id] = std::move(stat);
                continue;
            }
            ContainerStat last_stat = container_stats_[container_id];
            float load_usage = compute_load(
                last_stat.timestamp, last_stat.cpu_usage,
                stat.timestamp, stat.cpu_usage);
            float user_load_stat = compute_load(
                last_stat.timestamp, tick_to_ns(last_stat.cpu_stat_user),
                stat.timestamp, tick_to_ns(stat.cpu_stat_user));
            float sys_load_stat = compute_load(
                last_stat.timestamp, tick_to_ns(last_stat.cpu_stat_sys),
                stat.timestamp, tick_to_ns(stat.cpu_stat_sys));
            if (entry.first == -1) {
                HLOG(INFO) << fmt::format(
                    "Gateway load: usage={}, user_stat={}, sys_stat={}",
                    load_usage, user_load_stat, sys_load_stat);
            } else {
                HLOG(INFO) << fmt::format(
                    "FuncContainer[{}] load: usage={}, user_stat={}, sys_stat={}",
                    entry.first, load_usage, user_load_stat, sys_load_stat);
            }
            total_load_usage += load_usage;
            total_user_load_stat += user_load_stat;
            total_sys_load_stat += sys_load_stat;
            container_stats_[container_id] = std::move(stat);
        }
        HLOG(INFO) << fmt::format(
            "Total load: usage={}, user_stat={}, sys_stat={}",
            total_load_usage, total_user_load_stat, total_sys_load_stat);
    }

    state_.store(kStopped);
}

bool Monitor::ReadContainerStat(std::string_view container_id, ContainerStat* stat) {
    stat->timestamp = GetMonotonicNanoTimestamp();
    if (!docker_utils::ReadCpuAcctUsage(container_id, &stat->cpu_usage)) {
        return false;
    }
    if (!docker_utils::ReadCpuAcctStat(container_id, &stat->cpu_stat_user, &stat->cpu_stat_sys)) {
        return false;
    }
    return true;
}

}  // namespace gateway
}  // namespace faas
