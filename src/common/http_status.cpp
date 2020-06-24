#include "common/http_status.h"

namespace faas {

std::string_view GetHttpStatusString(HttpStatus status) {
    switch (status) {
    case HttpStatus::OK:
        return "200 OK";
    case HttpStatus::BAD_REQUEST:
        return "400 Bad Request";
    case HttpStatus::NOT_FOUND:
        return "404 Not Found";
    case HttpStatus::INTERNAL_SERVER_ERROR:
        return "500 Internal Server Error";
    default:
        LOG(FATAL) << "Unknown HTTP status: " << static_cast<int>(status);
    }
}

}  // namespace faas
