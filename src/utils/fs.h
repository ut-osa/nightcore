#pragma once

#include "base/common.h"

namespace faas {
namespace fs_utils {

bool Exists(absl::string_view path);
bool IsFile(absl::string_view path);
bool IsDirectory(absl::string_view path);
bool MakeDirectory(absl::string_view path, mode_t mode = 0777);
bool Remove(absl::string_view path);
bool RemoveDirectoryRecursively(absl::string_view path);

}  // namespace utils
}  // namespace faas
