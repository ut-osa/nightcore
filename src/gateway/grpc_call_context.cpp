#include "gateway/grpc_call_context.h"

#include "gateway/grpc_connection.h"

namespace faas {
namespace gateway {

bool GrpcCallContext::Finish() {
    absl::MutexLock lk(&mu_);
    if (connection_ != nullptr) {
        connection_->GrpcCallFinish(this);
        return true;
    }
    return false;
}

void GrpcCallContext::OnStreamClose() {
    absl::MutexLock lk(&mu_);
    DCHECK(connection_ != nullptr);
    connection_ = nullptr;
}

}  // namespace gateway
}  // namespace faas
