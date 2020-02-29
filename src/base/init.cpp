#include "base/init.h"

#include <absl/flags/parse.h>
#include <absl/flags/flag.h>
#include <absl/debugging/symbolize.h>
#include <absl/debugging/failure_signal_handler.h>
#include <glog/logging.h>

ABSL_FLAG(int, glog_v, 0, "");

namespace faas {
namespace base {

void InitMain(int argc, char* argv[]) {
    absl::InitializeSymbolizer(argv[0]);
    absl::FailureSignalHandlerOptions options;
    absl::InstallFailureSignalHandler(options);

    std::vector<char*> unparsed_args = absl::ParseCommandLine(argc, argv);

    FLAGS_logtostderr = 1;
    FLAGS_v = absl::GetFlag(FLAGS_glog_v);
    google::InitGoogleLogging(argv[0]);

    if (unparsed_args.size() > 1) {
        for (size_t i = 1; i < unparsed_args.size(); i++) {
            LOG(WARNING) << "Unrecognized argument: " << unparsed_args[i];
        }
    }
}

}  // namespace base
}  // namespace faas
