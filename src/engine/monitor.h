#pragma once

#include "base/common.h"
#include "base/thread.h"

namespace faas {
namespace engine {

class Engine;
class MessageConnection;

class Monitor {
public:
    static constexpr float kDefaultFrequencyHz = 0.3;

    explicit Monitor(Engine* engine);
    ~Monitor();

    void set_frequency(float hz);

    void Start();
    void ScheduleStop();
    void WaitForFinish();

    void OnIOWorkerCreated(std::string_view worker_name, int event_loop_thread_tid);
    void OnNewFuncContainer(uint16_t func_id, std::string_view container_id);

private:
    enum State { kCreated, kRunning, kStopping, kStopped };
    std::atomic<State> state_;
    Engine* engine_;
    float frequency_hz_;
    base::Thread background_thread_;

    absl::Mutex mu_;
    std::string self_container_id_;
    absl::flat_hash_map</* tid */ int, /* worker_name */ std::string>
        io_workers_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map</* func_id */ uint16_t, std::string>
        func_container_ids_ ABSL_GUARDED_BY(mu_);

    void BackgroundThreadMain();

    DISALLOW_COPY_AND_ASSIGN(Monitor);
};

}  // namespace engine
}  // namespace faas
