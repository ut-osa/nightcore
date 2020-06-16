#pragma once

#include "base/common.h"
#include "base/thread.h"

namespace faas {
namespace gateway {

class Server;
class MessageConnection;

class Monitor {
public:
    static constexpr float kDefaultFrequencyHz = 0.3;

    explicit Monitor(Server* server);
    ~Monitor();

    void set_frequency(float hz);

    void Start();
    void ScheduleStop();
    void WaitForFinish();

    void OnLauncherConnected(MessageConnection* launcher_connection,
                             std::string_view container_id);

private:
    enum State { kCreated, kRunning, kStopping, kStopped };
    std::atomic<State> state_;
    Server* server_;
    float frequency_hz_;
    base::Thread background_thread_;

    absl::Mutex mu_;
    std::string self_container_id_;
    absl::flat_hash_map</* func_id */ uint16_t, std::string>
        func_container_ids_ ABSL_GUARDED_BY(mu_);

    struct ContainerStat {
        int64_t timestamp;
        int64_t cpu_usage;      // in ns, from cpuacct.usage
        int32_t cpu_stat_user;  // in tick, from cpuacct.stat
        int32_t cpu_stat_sys;   // in tick, from cpuacct.stat
    };

    void BackgroundThreadMain();
    bool ReadContainerStat(std::string_view container_id, ContainerStat* stat);

    DISALLOW_COPY_AND_ASSIGN(Monitor);
};

}  // namespace gateway
}  // namespace faas
