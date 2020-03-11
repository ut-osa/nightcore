#pragma once

#include <vector>

namespace faas {
namespace base {

void InitMain(int argc, char* argv[],
              std::vector<char*>* positional_args = nullptr);

void InitGoogleLogging(const char* argv0);

}  // namespace base
}  // namespace faas
