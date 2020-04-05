#include <nan.h>
#include "worker_manager.h"

void InitAll(v8::Local<v8::Object> exports) {
    WorkerManager::Init(exports);
}

NODE_MODULE(NODE_GYP_MODULE_NAME, InitAll)
