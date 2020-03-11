#include "utils/fs.h"

#include <fcntl.h>
#include <ftw.h>

namespace faas {
namespace fs_utils {

namespace {
bool Stat(absl::string_view path, struct stat* statbuf) {
    return stat(std::string(path).c_str(), statbuf) == 0;
}

int RemoveFileFtwFn(const char* fpath, const struct stat* sb,
                    int typeflag, struct FTW *ftwbuf) {
    return remove(fpath) != 0;
}
}

bool Exists(absl::string_view path) {
    return access(std::string(path).c_str(), F_OK) == 0;
}

bool IsFile(absl::string_view path) {
    struct stat statbuf;
    if (!Stat(path, &statbuf)) {
        return false;
    }
    return S_ISREG(statbuf.st_mode) != 0;
}

bool IsDirectory(absl::string_view path) {
    struct stat statbuf;
    if (!Stat(path, &statbuf)) {
        return false;
    }
    return S_ISDIR(statbuf.st_mode) != 0;
}

bool MakeDirectory(absl::string_view path, mode_t mode) {
    return mkdir(std::string(path).c_str(), mode) == 0;
}

bool Remove(absl::string_view path) {
    return remove(std::string(path).c_str()) == 0;
}

bool RemoveDirectoryRecursively(absl::string_view path) {
    return nftw(std::string(path).c_str(), RemoveFileFtwFn, 8,
                FTW_DEPTH|FTW_MOUNT|FTW_PHYS) == 0;
}

}  // namespace utils
}  // namespace faas
