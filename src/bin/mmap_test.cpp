#include "base/init.h"
#include "base/common.h"

#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <absl/flags/flag.h>

ABSL_FLAG(std::string, base_path, "/dev/shm/test", "");
ABSL_FLAG(size_t, mmap_size, 4096, "");
ABSL_FLAG(int, repeat_times, 1, "");

uint64_t CurrentTimestamp() {
    struct timespec tp;
    CHECK_EQ(clock_gettime(CLOCK_MONOTONIC, &tp), 0) << "clock_gettime failed";
    uint64_t ret = 0;
    ret += static_cast<uint64_t>(tp.tv_sec) * 1000000;
    ret += static_cast<uint64_t>(tp.tv_nsec) / 1000;
    return ret;
}

int main(int argc, char* argv[]) {
    faas::base::InitMain(argc, argv);

    int repeat_times = absl::GetFlag(FLAGS_repeat_times);
    std::string base_path = absl::GetFlag(FLAGS_base_path);
    int mmap_size = absl::GetFlag(FLAGS_mmap_size);

    uint64_t start_time = CurrentTimestamp();

    for (int i = 0; i < repeat_times; i++) {
        std::string path = absl::StrFormat("%s/%d", base_path, i);
        int fd = open(path.c_str(), O_CREAT|O_TRUNC|O_RDWR, 0644);
        CHECK(fd != -1);
        CHECK(ftruncate(fd, mmap_size) == 0);
        void* ptr = mmap(0, mmap_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
        CHECK(ptr != MAP_FAILED);
        close(fd);
        memset(ptr, 0, mmap_size);
    }

    uint64_t elapsed_time = CurrentTimestamp() - start_time;
    double rate = repeat_times / (elapsed_time / 1000000.0);
    std::cout << "Rate: " << rate << " calls per sec" << std::endl;

    return 0;
}
