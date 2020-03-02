#pragma once

#include "base/common.h"
#include <pthread.h>

namespace faas {
namespace base {

class Thread {
public:
    Thread(const std::string& name, std::function<void()> fn)
        : state_(kReady), name_(name), fn_(fn), tid_(-1) {}

    ~Thread() {
        State state = state_.load();
        CHECK(state == kReady || state == kFinished);
    }

    void Start();
    void Join();

    const std::string& name() const { return name_; }
    int tid() { return tid_; }
    static Thread* current() { return current_; }

private:
    enum State { kReady, kStarting, kRunning, kFinished };

    std::atomic<State> state_;
    std::string name_;
    std::function<void()> fn_;
    int tid_;

    absl::Notification started_;
    pthread_t pthread_;

    static thread_local Thread* current_;

    void Run();
    static void* StartRoutine(void* arg);

    DISALLOW_COPY_AND_ASSIGN(Thread);
};

}  // namespace base
}  // namespace faas
