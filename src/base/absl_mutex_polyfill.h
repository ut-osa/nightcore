#pragma once

#ifdef __FAAS_HAVE_ABSL

#include <absl/synchronization/mutex.h>

#else  // __FAAS_HAVE_ABSL

// No-op polyfills for thread annotations
#define ABSL_GUARDED_BY(x)
#define ABSL_EXCLUSIVE_LOCKS_REQUIRED(x)

#include "base/macro.h"  // For DISALLOW_COPY_AND_ASSIGN

namespace absl {

#ifdef __FAAS_NODE_ADDON
// Node.js environment can safely use an no-op mutex

class Mutex {
public:
    Mutex() {}
    ~Mutex() {}
    void Lock() {}
    void Unlock() {}
private:
    DISALLOW_COPY_AND_ASSIGN(Mutex);
};

#elif defined(__FAAS_PYTHON_BINDING)
// Python environment can safely use an no-op mutex

class Mutex {
public:
    Mutex() {}
    ~Mutex() {}
    void Lock() {}
    void Unlock() {}
private:
    DISALLOW_COPY_AND_ASSIGN(Mutex);
};

#else

#error No polyfill implementation available

#endif

class MutexLock {
public:
    explicit MutexLock(Mutex *mu) : mu_(mu) { this->mu_->Lock(); }
    ~MutexLock() { this->mu_->Unlock(); }
private:
    Mutex *const mu_;
    DISALLOW_COPY_AND_ASSIGN(MutexLock);
};

}  // namespace absl

#endif  // __FAAS_HAVE_ABSL
