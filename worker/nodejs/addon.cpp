#define __FAAS_NODE_ADDON_SRC
#include "base/logging.h"
#include "utils/env_variables.h"
#include "engine.h"

#include <napi.h>

namespace faas {
namespace nodejs {

Napi::Object InitModule(Napi::Env env, Napi::Object exports) {
    logging::Init(utils::GetEnvVariableAsInt("FAAS_VLOG_LEVEL", 0));
    return Engine::Init(env, exports);
}

}  // namespace nodejs
}  // namespace faas

Napi::Object InitAll(Napi::Env env, Napi::Object exports) {
    return faas::nodejs::InitModule(env, exports);
}

NODE_API_MODULE(addon, InitAll)
