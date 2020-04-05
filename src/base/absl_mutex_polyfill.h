#pragma once

#ifdef __FAAS_HAVE_ABSL

#include <absl/synchronization/mutex.h>

#else  // __FAAS_HAVE_ABSL

// No-op polyfills for thread annotations
#define ABSL_GUARDED_BY(x)
#define ABSL_EXCLUSIVE_LOCKS_REQUIRED(x)

#ifdef __FAAS_NODE_ADDON
// Node.js environment can safely use an no-op polyfill

#include "base/macro.h"  // For DISALLOW_COPY_AND_ASSIGN

namespace absl {

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

}  // namespace absl

#endif  // __FAAS_NODE_ADDON

#endif  // __FAAS_HAVE_ABSL
