#pragma once

#include "base/common.h"

namespace faas {
namespace utils {

int UnixDomainSocketConnect(absl::string_view path);

}  // namespace utils
}  // namespace faas
