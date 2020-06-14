#include "base/asm.h"

#define LOAD(T, ptr) *reinterpret_cast<T*>(ptr)
#define STORE(T, ptr, value) *reinterpret_cast<T*>(ptr) = (value)

namespace faas {
namespace ipc {

template<class T>
std::unique_ptr<SPSCQueue<T>> SPSCQueue<T>::Create(std::string_view name, size_t queue_size) {
    CHECK_GE(queue_size, 2U) << "Queue size must be at least 2";
    auto region = ShmCreate(fmt::format("SPSCQueue_{}", name), compute_total_bytesize(queue_size));
    CHECK(region != nullptr) << "ShmCreate failed";
    BuildMemoryLayout(region->base(), queue_size);
    return std::unique_ptr<SPSCQueue<T>>(new SPSCQueue<T>(true, std::move(region)));
}

template<class T>
std::unique_ptr<SPSCQueue<T>> SPSCQueue<T>::Open(std::string_view name) {
    auto region = ShmOpen(fmt::format("SPSCQueue_{}", name), /* readonly= */ false);
    CHECK(region != nullptr) << "ShmOpen failed";
    return std::unique_ptr<SPSCQueue<T>>(new SPSCQueue<T>(false, std::move(region)));
}

template<class T>
SPSCQueue<T>::SPSCQueue(bool consumer, std::unique_ptr<ShmRegion> shm_region) {
    consumer_ = consumer;
    shm_region_ = std::move(shm_region);
    if (consumer_) {
        shm_region_->EnableRemoveOnDestruction();
    }
    char* base_ptr = shm_region_->base();
    size_t message_size = LOAD(size_t, base_ptr);
    CHECK_EQ(message_size, sizeof(T));
    queue_size_ = LOAD(size_t, base_ptr + sizeof(size_t));
    CHECK_EQ(shm_region_->size(), compute_total_bytesize(queue_size_));
    head_ = reinterpret_cast<size_t*>(base_ptr + __FAAS_CACHE_LINE_SIZE);
    tail_ = reinterpret_cast<size_t*>(base_ptr + __FAAS_CACHE_LINE_SIZE * 2);
    cell_base_ = reinterpret_cast<char*>(base_ptr + __FAAS_CACHE_LINE_SIZE * 3);
    wakeup_consumer_flag_ = false;
}

template<class T>
SPSCQueue<T>::~SPSCQueue() {}

template<class T>
size_t SPSCQueue<T>::compute_total_bytesize(size_t queue_size) {
    return __FAAS_CACHE_LINE_SIZE * 3 + sizeof(T) * queue_size;
}

template<class T>
void SPSCQueue<T>::BuildMemoryLayout(char* base_ptr, size_t queue_size) {
    STORE(size_t, base_ptr, sizeof(T));                      // message_size
    STORE(size_t, base_ptr + sizeof(size_t), queue_size);    // queue_size
    STORE(size_t, base_ptr + __FAAS_CACHE_LINE_SIZE, 0);     // head
    STORE(size_t, base_ptr + __FAAS_CACHE_LINE_SIZE * 2, 0); // tail
}

template<class T>
bool SPSCQueue<T>::Push(const T& message) {
    DCHECK(!consumer_);
    size_t current = __atomic_load_n(tail_, __ATOMIC_RELAXED);
    size_t next = current + 1;
    if (next == queue_size_) {
        next = 0;
    }
    size_t head = __atomic_load_n(head_, __ATOMIC_ACQUIRE);
    if ((head & kConsumerSleepMask) == kConsumerSleepMask) {
        if (!wakeup_consumer_flag_) {
            VLOG(1) << "Consumer is sleeping, and will call wake function";
            wakeup_consumer_flag_ = true;
            wakeup_consumer_fn_();
        }
        head ^= kConsumerSleepMask;
    } else {
        wakeup_consumer_flag_ = false;
    }
    if (next != head) {
        STORE(T, cell(current), message);
        __atomic_store_n(tail_, next, __ATOMIC_RELEASE);
        asm_volatile_memory();
        return true;
    } else {
        // Queue is full
        return false;
    }
}

template<class T>
void SPSCQueue<T>::SetWakeupConsumerFn(std::function<void()> fn) {
    DCHECK(!consumer_);
    wakeup_consumer_fn_ = fn;
}

template<class T>
bool SPSCQueue<T>::Pop(T* message) {
    DCHECK(consumer_);
    size_t current = __atomic_load_n(head_, __ATOMIC_RELAXED);
    if ((current & kConsumerSleepMask) == kConsumerSleepMask) {
        current ^= kConsumerSleepMask;
        __atomic_fetch_xor(head_, kConsumerSleepMask, __ATOMIC_RELEASE);
        asm_volatile_memory();
    }
    if (current == __atomic_load_n(tail_, __ATOMIC_ACQUIRE)) {
        // Queue is empty
        return false;
    } else {
        size_t next = current + 1;
        if (next == queue_size_) {
            next = 0;
        }
        *message = LOAD(T, cell(current));
        __atomic_store_n(head_, next, __ATOMIC_RELEASE);
        asm_volatile_memory();
        return true;
    }
}

template<class T>
void SPSCQueue<T>::ConsumerEnterSleep() {
    DCHECK(consumer_);
    __atomic_fetch_or(head_, kConsumerSleepMask, __ATOMIC_RELEASE);
    asm_volatile_memory();
}

}  // namespace ipc
}  // namespace faas

#undef STORE
#undef LOAD
