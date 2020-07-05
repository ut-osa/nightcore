{
  "targets": [
    {
      "target_name": "addon",
      "sources": [
        "addon.cpp",
        "engine.cpp",
        "src/base/logging.cpp",
        "src/common/func_config.cpp",
        "src/ipc/base.cpp",
        "src/ipc/fifo.cpp",
        "src/ipc/shm_region.cpp",
        "src/utils/fs.cpp",
        "src/utils/socket.cpp",
        "src/worker/worker_lib.cpp",
        "src/worker/event_driven_worker.cpp"
      ],
      "include_dirs": [
        "<!@(node -p \"require('node-addon-api').include\")",
        "./src",
        "./deps/fmt/include",
        "./deps/GSL/include",
        "./deps/json/single_include"
      ],
      "defines": [
        "NAPI_DISABLE_CPP_EXCEPTIONS",
        "FMT_HEADER_ONLY",
        "__FAAS_NODE_ADDON",
        "DCHECK_ALWAYS_ON"
      ],
      "cflags!": [ "-fno-exceptions" ],
      "cflags_cc!": [ "-fno-exceptions" ],
      "cflags_cc": [ "-std=c++17" ]
    }
  ]
}
