#pragma once

#include "base/common.h"

namespace faas {
namespace fs_utils {

bool Exists(std::string_view path);
bool IsFile(std::string_view path);
bool IsDirectory(std::string_view path);
std::string GetRealPath(std::string_view path);
bool MakeDirectory(std::string_view path);
bool Remove(std::string_view path);
bool RemoveDirectoryRecursively(std::string_view path);
bool ReadContents(std::string_view path, std::string* contents);

std::string JoinPath(std::string_view path1, std::string_view path2);
std::string JoinPath(std::string_view path1, std::string_view path2, std::string_view path3);

}  // namespace fs_utils
}  // namespace faas
