{
  "targets": [
    {
      "target_name": "addon",
      "sources": [
        "addon.cpp",
        "worker_manager.cpp",
        "src/base/logging.cpp",
        "src/common/func_config.cpp",
        "src/common/stat.cpp",
        "src/utils/shared_memory.cpp",
        "src/utils/fs.cpp",
        "src/worker/lib/manager.cpp"
      ],
      "include_dirs": [
        "<!(node -e \"require('nan')\")",
        "./src",
        "./deps/fmt/include",
        "./deps/GSL/include",
        "./deps/json/single_include"
      ],
      "defines": [ "FMT_HEADER_ONLY", "__FAAS_NODE_ADDON", "DCHECK_ALWAYS_ON" ],
      "cflags_cc": [ "-std=c++17" ]
    }
  ]
}
