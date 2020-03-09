#pragma once

#include <vector>

namespace faas {
namespace base {

void InitMain(int argc, char* argv[],
              std::vector<char*>* positional_args = nullptr);

}  // namespace base
}  // namespace faas
