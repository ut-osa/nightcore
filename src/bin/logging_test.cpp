#include "base/init.h"
#include "base/common.h"

#include <errno.h>

#include <absl/flags/flag.h>

int foo() {
    LOG(INFO) << "inner";
    return 1;
}

int main(int argc, char* argv[]) {
    faas::base::InitMain(argc, argv);

    // CHECK_EQ(1, 1) << "Hi";
    
    // errno = EDEADLK;
    // PLOG(FATAL) << "GGG";

    // LOG(FATAL) << foo();

    DCHECK(1 == 2) << "GOOG";

    return 0;
}
