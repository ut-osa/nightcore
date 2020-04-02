#pragma once

namespace faas {

enum class HttpStatus {
    OK = 200,
    BAD_REQUEST = 400,
    NOT_FOUND = 404,
    INTERNAL_SERVER_ERROR = 500
};

}  // namespace faas
