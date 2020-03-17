#include "common/func_config.h"

#include <yaml-cpp/yaml.h>

namespace faas {

constexpr int FuncConfig::kMaxFuncId;

bool FuncConfig::is_valid_func_id(int func_id) {
    return 0 < func_id && func_id <= kMaxFuncId;
}

bool FuncConfig::is_valid_func_name(absl::string_view func_name) {
    for (const char& ch : func_name) {
        if (!(('0' <= ch && ch <= '9') ||
              ('a' <= ch && ch <= 'z') ||
              ('A' <= ch && ch <= 'Z'))) {
            return false;
        }
    }
    return true;
}

bool FuncConfig::Load(absl::string_view yaml_path) {
    YAML::Node config;
    try {
        config = YAML::LoadFile(std::string(yaml_path));
    } catch (const YAML::Exception& e) {
        LOG(ERROR) << "Failed to load YAML from file " << yaml_path << ": "
                   << e.msg;
        return false;
    }
    if (config.Type() != YAML::NodeType::Map) {
        LOG(ERROR) << "Invalid config file";
        return false;
    }
    try {
        for (const auto& item : config) {
            std::string func_name = item.first.as<std::string>();
            if (!is_valid_func_name(func_name)) {
                LOG(ERROR) << "Invalid func_name: " << func_name;
                return false;
            }
            const YAML::Node& func_config = item.second;
            int func_id = func_config["func_id"].as<int>();
            if (!is_valid_func_id(func_id)) {
                LOG(ERROR) << "Invalid func_id: " << func_id;
                return false;
            }
            LOG(INFO) << "Load configuration for function " << func_name
                      << "[" << func_id << "]";
            auto entry = absl::make_unique<Entry>();
            entry->func_name = func_name;
            entry->func_id = func_id;
            entires_by_func_name_[func_name] = entry.get();
            entries_by_func_id_[func_id] = entry.get();
            entries_.push_back(std::move(entry));
        }
    } catch (const YAML::Exception& e) {
        LOG(ERROR) << "Invalid config file: " << e.msg;
        return false;
    }
    return true;
}

}  // namespace faas
