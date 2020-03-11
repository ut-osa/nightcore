#include "base/init.h"

#include <absl/flags/flag.h>

namespace google {
extern void InitGoogleLogging(const char* argv0);
}

#define DECLARE_VARIABLE(type, shorttype, name, tn)  \
  namespace fL##shorttype {                          \
    extern type FLAGS_##name;                        \
  }
#define DECLARE_bool(name) \
  DECLARE_VARIABLE(bool, B, name, bool)
#define DECLARE_int32(name) \
  DECLARE_VARIABLE(int32_t, I, name, int32)
#define DECLARE_string(name)              \
  namespace fLS {                         \
    extern std::string& FLAGS_##name;     \
  }

DECLARE_bool(logtostderr);
DECLARE_int32(stderrthreshold);
DECLARE_int32(minloglevel);
DECLARE_string(log_dir);
DECLARE_int32(v);

ABSL_FLAG(bool, logtostderr, true,
          "log messages go to stderr instead of logfiles");
ABSL_FLAG(int, stderrthreshold, 2 /* ERROR */,
          "log messages at or above this level are copied to stderr in addition to logfiles");
ABSL_FLAG(int, minloglevel, 0 /* INFO */,
          "Messages logged at a lower level than this don't actually get logged anywhere");
ABSL_FLAG(std::string, log_dir, "",
          "If specified, logfiles are written into this directory instead of the default logging directory.");
ABSL_FLAG(int, v, 0,
          "Show all VLOG(m) messages for m <= this.");

namespace faas {
namespace base {

void InitGoogleLogging(const char* argv0) {
    fLB::FLAGS_logtostderr = absl::GetFlag(FLAGS_logtostderr);
    fLI::FLAGS_stderrthreshold = absl::GetFlag(FLAGS_stderrthreshold);
    fLI::FLAGS_minloglevel = absl::GetFlag(FLAGS_minloglevel);
    fLS::FLAGS_log_dir = absl::GetFlag(FLAGS_log_dir);
    fLI::FLAGS_v = absl::GetFlag(FLAGS_v);
    google::InitGoogleLogging(argv0);
}

}  // namespace base
}  // namespace faas
