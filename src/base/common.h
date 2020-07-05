#pragma once

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#ifdef __FAAS_NODE_ADDON
#if !defined(__FAAS_USED_IN_BINDING) && !defined(__FAAS_NODE_ADDON_SRC)
#error Need the source file to have __FAAS_USED_IN_BINDING defined
#endif
#define __FAAS_CXX_NO_EXCEPTIONS
#endif

#ifdef __FAAS_PYTHON_BINDING
#if !defined(__FAAS_USED_IN_BINDING) && !defined(__FAAS_PYTHON_BINDING_SRC)
#error Need the source file to have __FAAS_USED_IN_BINDING defined
#endif
#include <Python.h>
#if PY_VERSION_HEX < 0x03070000
#error FaaS Python binding requires Python 3.7+
#endif
#endif

// C includes
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <unistd.h>

// C++ includes
#include <limits>
#include <atomic>
#include <memory>
#include <utility>
#include <sstream>
#include <string>
#include <string_view>
#include <functional>
#include <algorithm>

// STL containers
#include <vector>
#include <queue>
#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>

// fmtlib
#include <fmt/core.h>
#include <fmt/format.h>

// Guidelines Support Library (GSL)
#include <gsl/gsl>

#ifdef __FAAS_SRC
#define __FAAS_HAVE_ABSL
#endif

#if defined(__FAAS_HAVE_ABSL) && !defined(__FAAS_USED_IN_BINDING)

// Will not include common absl headers in source files
// with __FAAS_USED_IN_BINDING defined

#include <absl/time/time.h>
#include <absl/time/clock.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/match.h>
#include <absl/strings/strip.h>
#include <absl/strings/str_split.h>
#include <absl/strings/numbers.h>
#include <absl/strings/ascii.h>
#include <absl/random/random.h>
#include <absl/random/distributions.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/container/inlined_vector.h>
#include <absl/synchronization/mutex.h>
#include <absl/synchronization/notification.h>
#include <absl/functional/bind_front.h>
#include <absl/algorithm/container.h>

#endif  // defined(__FAAS_HAVE_ABSL) && !defined(__FAAS_USED_IN_BINDING)

#include "base/macro.h"
#include "base/logging.h"
#include "base/std_span_polyfill.h"
