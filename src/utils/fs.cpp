#define __FAAS_USED_IN_BINDING
#include "utils/fs.h"

#include <fcntl.h>
#include <ftw.h>

namespace faas {
namespace fs_utils {

namespace {
bool Stat(std::string_view path, struct stat* statbuf) {
    return stat(std::string(path).c_str(), statbuf) == 0;
}

int RemoveFileFtwFn(const char* fpath, const struct stat* sb,
                    int typeflag, struct FTW *ftwbuf) {
    return remove(fpath) != 0;
}
}

bool Exists(std::string_view path) {
    return access(std::string(path).c_str(), F_OK) == 0;
}

bool IsFile(std::string_view path) {
    struct stat statbuf;
    if (!Stat(path, &statbuf)) {
        return false;
    }
    return S_ISREG(statbuf.st_mode) != 0;
}

bool IsDirectory(std::string_view path) {
    struct stat statbuf;
    if (!Stat(path, &statbuf)) {
        return false;
    }
    return S_ISDIR(statbuf.st_mode) != 0;
}

std::string GetRealPath(std::string_view path) {
    char* result = realpath(std::string(path).c_str(), nullptr);
    if (result == nullptr) {
        LOG(WARNING) << path << " is not a valid path";
        return std::string(path);
    }
    std::string result_str(result);
    free(result);
    return result_str;
}

bool MakeDirectory(std::string_view path) {
    return mkdir(std::string(path).c_str(), __FAAS_DIR_CREAT_MODE) == 0;
}

bool Remove(std::string_view path) {
    return remove(std::string(path).c_str()) == 0;
}

bool RemoveDirectoryRecursively(std::string_view path) {
    return nftw(std::string(path).c_str(), RemoveFileFtwFn, 8,
                FTW_DEPTH|FTW_MOUNT|FTW_PHYS) == 0;
}

bool ReadContents(std::string_view path, std::string* contents) {
    FILE* fin = fopen(std::string(path).c_str(), "rb");
    if (fin == nullptr) {
        LOG(ERROR) << "Failed to open file: " << path;
        return false;
    }
    auto close_file = gsl::finally([fin] { fclose(fin); });
    char buffer[128];
    contents->clear();
    while (feof(fin) == 0) {
        size_t nread = fread(buffer, 1, sizeof(buffer), fin);
        if (nread > 0) {
            contents->append(buffer, nread);
        } else {
            break;
        }
    }
    return true;
}

std::string JoinPath(std::string_view path1, std::string_view path2) {
    return fmt::format("{}/{}", path1, path2);
}

std::string JoinPath(std::string_view path1, std::string_view path2, std::string_view path3) {
    return fmt::format("{}/{}/{}", path1, path2, path3);
}

}  // namespace fs_utils
}  // namespace faas
