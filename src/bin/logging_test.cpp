#include "base/init.h"
#include "base/common.h"

#include <errno.h>
#include <absl/flags/flag.h>

int main(int argc, char* argv[]) {
    faas::base::InitMain(argc, argv);

    LOG(INFO) << "Hello";
    errno = 5;
    PLOG(INFO) << "GOOO";

    CHECK_EQ('a', '\0') << "Boom";

    return 0;
}
