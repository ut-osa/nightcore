#pragma once

#include "base/common.h"
#include <pthread.h>

namespace faas {
namespace base {

class Thread {
public:
    Thread(absl::string_view name, std::function<void()> fn)
        : state_(kCreated), name_(std::string(name)), fn_(fn), tid_(-1) {}

    ~Thread() {
        State state = state_.load();
        DCHECK(state == kCreated || state == kFinished);
    }

    void Start();
    void Join();

    absl::string_view name() const { return name_; }
    int tid() { return tid_; }
    static Thread* current() {
        DCHECK(current_ != nullptr);
        return current_;
    }

    static void RegisterMainThread();

private:
    enum State { kCreated, kStarting, kRunning, kFinished };

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
