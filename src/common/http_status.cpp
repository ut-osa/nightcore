#include "common/http_status.h"

namespace faas {

std::string_view GetHttpStatusString(HttpStatus status) {
    switch (status) {
    case HttpStatus::OK:
        return "OK";
    case HttpStatus::BAD_REQUEST:
        return "Bad Request";
    case HttpStatus::NOT_FOUND:
        return "Not Found";
    case HttpStatus::INTERNAL_SERVER_ERROR:
        return "Internal Server Error";
    default:
        LOG(FATAL) << "Unknown HTTP status: " << static_cast<int>(status);
    }
}

}  // namespace faas
