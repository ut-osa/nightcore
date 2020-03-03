#pragma once

#include "base/common.h"

namespace faas {
namespace utils {

template<class T>
T* DefaultObjectConstructor() {
    return new T();
}

template<class T>
class ObjectPool {
public:
    ObjectPool(size_t initial_size, size_t max_size,
               std::function<T*()> object_constructor = DefaultObjectConstructor<T>)
        : max_size_(max_size), tail_(initial_size), pool_(new std::shared_ptr<T>[max_size]),
          object_constructor_(object_constructor) {
        CHECK_LE(initial_size, max_size);
        for (size_t i = 0; i < initial_size; i++) {
            pool_[i] = std::shared_ptr<T>(object_constructor());
        }
    }

    ~ObjectPool() {
        delete[] pool_;
    }

    std::shared_ptr<T> Get() {
        std::shared_ptr<T> obj = nullptr;
        {
            absl::MutexLock lk(&mu_);
            if (tail_ > 0) {
                obj = std::move(pool_[--tail_]);
            }
        }
        if (obj == nullptr) {
            obj = std::shared_ptr<T>(object_constructor_());
        }
        return obj;
    }

    void Return(std::shared_ptr<T> obj) {
        absl::MutexLock lk(&mu_);
        if (tail_ < max_size_) {
            pool_[tail_++] = std::move(obj);
        }
    }

private:
    size_t max_size_;
    size_t tail_;
    std::shared_ptr<T>* pool_;
    std::function<T*()> object_constructor_;
    absl::Mutex mu_;

    DISALLOW_COPY_AND_ASSIGN(ObjectPool);
};

}  // namespace utils
}  // namespace faas
