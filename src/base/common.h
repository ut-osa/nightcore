#pragma once

#ifdef __FAAS_NODE_ADDON
#ifndef __FAAS_USED_IN_BINDING
#error Need the source file to have __FAAS_USED_IN_BINDING defined
#endif
#endif

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <unistd.h>

#include <string>
#include <string_view>
#include <vector>
#include <queue>
#include <sstream>
#include <atomic>
#include <functional>
#include <algorithm>
#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>

#include <gsl/span>
namespace std {
using gsl::span;
}  // namespace std

#include "base/macro.h"
#include "base/logging.h"

#if defined(__FAAS_SRC) && !defined(__FAAS_USED_IN_BINDING)

#include <absl/time/time.h>
#include <absl/time/clock.h>
#include <absl/strings/str_format.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/match.h>
#include <absl/strings/strip.h>
#include <absl/strings/str_split.h>
#include <absl/strings/numbers.h>
#include <absl/random/random.h>
#include <absl/random/distributions.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/container/inlined_vector.h>
#include <absl/synchronization/mutex.h>
#include <absl/synchronization/notification.h>
#include <absl/functional/bind_front.h>
#include <absl/algorithm/container.h>

#include "base/thread.h"

#else

#include "base/absl_mutex_polyfill.h"

#endif  // defined(__FAAS_SRC) && !defined(__FAAS_USED_IN_BINDING)
