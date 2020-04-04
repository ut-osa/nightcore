#pragma once

#ifdef __FAAS_SRC

#include <absl/synchronization/mutex.h>

#else  // __FAAS_SRC

namespace absl {

// No-op polyfills

class Mutex {
public:
    Mutex() {}
    ~Mutex() {}
    void Lock() {}
    void Unlock() {}
private:
    DISALLOW_COPY_AND_ASSIGN(Mutex);
};

class MutexLock {
public:
    MutexLock(Mutex*) {}
    ~MutexLock() {}
private:
    DISALLOW_COPY_AND_ASSIGN(MutexLock);
};

#define ABSL_GUARDED_BY(x)
#define ABSL_EXCLUSIVE_LOCKS_REQUIRED(x)

}  // namespace absl

#endif  // __FAAS_SRC
