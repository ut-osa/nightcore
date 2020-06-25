#include "common/uv.h"

namespace faas {
namespace uv {

void HandleFreeCallback(uv_handle_t* handle) {
    free(handle);
}

HandleScope::HandleScope()
    : loop_(nullptr), num_handles_on_closing_(0) {}

HandleScope::~HandleScope() {
    DCHECK(handles_.empty());
}

void HandleScope::Init(uv_loop_t* loop, std::function<void()> finish_callback) {
    DCHECK(loop_ == nullptr);
    loop_ = loop;
    finish_callback_ = finish_callback;
}

void HandleScope::AddHandle(uv_handle_t* handle) {
    DCHECK(handle->loop == loop_);
    handles_.insert(handle);
}

void HandleScope::CloseHandle(uv_handle_t* handle) {
    DCHECK_IN_EVENT_LOOP_THREAD(loop_);
    DCHECK(handles_.contains(handle));
    handles_.erase(handle);
    handle->data = this;
    num_handles_on_closing_++;
    uv_close(handle, &HandleScope::HandleCloseCallback);
}

UV_CLOSE_CB_FOR_CLASS(HandleScope, HandleClose) {
    num_handles_on_closing_--;
    if (num_handles_on_closing_ == 0 && handles_.empty()) {
        finish_callback_();
    }
}

}  // namespace uv
}  // namespace faas
