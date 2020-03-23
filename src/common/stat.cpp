#include "common/stat.h"

namespace faas {
namespace stat {

constexpr absl::Duration Counter::kDefaultReportInterval;
constexpr absl::Duration CategoryCounter::kDefaultReportInterval;

}  // namespace stat
}  // namespace faas
