#include "gateway/http_request_context.h"

#include "gateway/http_connection.h"

namespace faas {
namespace gateway {

bool HttpAsyncRequestContext::Finish() {
    absl::MutexLock lk(&mu_);
    if (connection_ != nullptr) {
        connection_->AsyncRequestFinish(this);
        return true;
    }
    return false;
}

void HttpAsyncRequestContext::OnConnectionClose() {
    absl::MutexLock lk(&mu_);
    DCHECK(connection_ != nullptr);
    connection_ = nullptr;
}

}  // namespace gateway
}  // namespace faas
