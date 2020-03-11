#include "base/thread.h"

#include <sys/syscall.h>

namespace faas {
namespace base {

thread_local Thread* Thread::current_ = nullptr;

namespace {
pid_t gettid() {
    return syscall(SYS_gettid);
}
}

void Thread::Start() {
    state_.store(kStarting);
    CHECK_EQ(pthread_create(&pthread_, nullptr, &Thread::StartRoutine, this), 0);
    started_.WaitForNotification();
    DCHECK(state_.load() == kRunning);
}

void Thread::Join() {
    State state = state_.load();
    if (state == kFinished) {
        return;
    }
    DCHECK(state == kRunning);
    CHECK_EQ(pthread_join(pthread_, nullptr), 0);
}

void Thread::Run() {
    tid_ = gettid();
    state_.store(kRunning);
    started_.Notify();
    LOG(INFO) << "Start thread: " << name_;
    fn_();
    state_.store(kFinished);
}

void* Thread::StartRoutine(void* arg) {
    Thread* self = reinterpret_cast<Thread*>(arg);
    current_ = self;
    self->Run();
    return nullptr;
}

}  // namespace base
}  // namespace faas
