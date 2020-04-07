#pragma once

#include "base/common.h"

namespace faas {
namespace fs_utils {

bool Exists(std::string_view path);
bool IsFile(std::string_view path);
bool IsDirectory(std::string_view path);
std::string GetRealPath(std::string_view path);
bool MakeDirectory(std::string_view path, mode_t mode = 0777);
bool Remove(std::string_view path);
bool RemoveDirectoryRecursively(std::string_view path);
bool ReadContents(std::string_view path, std::string* contents);

}  // namespace fs_utils
}  // namespace faas
