#define __FAAS_USED_IN_BINDING
#include "common/func_config.h"

#include "utils/fs.h"

#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace faas {

constexpr int FuncConfig::kMaxFuncId;

namespace {
bool StartsWith(std::string_view s, std::string_view prefix) {
    return s.find(prefix) == 0;
}

std::string_view StripPrefix(std::string_view s, std::string_view prefix) {
    if (!StartsWith(s, prefix)) {
        return s;
    } else {
        return s.substr(prefix.length());
    }
}
}

bool FuncConfig::ValidateFuncId(int func_id) {
    return 0 < func_id && func_id <= kMaxFuncId;
}

bool FuncConfig::ValidateFuncName(std::string_view func_name) {
    if (StartsWith(func_name, "grpc:")) {
        // gRPC service
        std::string_view service_name = StripPrefix(func_name, "grpc:");
        for (const char& ch : service_name) {
            if (!(('0' <= ch && ch <= '9') ||
                  ('a' <= ch && ch <= 'z') ||
                  ('A' <= ch && ch <= 'Z') ||
                  ch == '.' || ch == '_')) {
                return false;
            }
        }
    } else {
        // Normal function
        for (const char& ch : func_name) {
            if (!(('0' <= ch && ch <= '9') ||
                  ('a' <= ch && ch <= 'z') ||
                  ('A' <= ch && ch <= 'Z'))) {
                return false;
            }
        }
    }
    return true;
}

bool FuncConfig::Load(std::string_view json_path) {
    std::string json_contents;
    if (!fs_utils::ReadContents(json_path, &json_contents)) {
        LOG(ERROR) << "Failed to read from file " << json_path;
        return false;
    }
    json config;
#ifndef __FAAS_NODE_ADDON
    try {
#endif
        config = json::parse(json_contents);
#ifndef __FAAS_NODE_ADDON
    } catch (const json::parse_error& e) {
        LOG(ERROR) << "Failed to parse json: " << e.what();
        return false;
    }
    try {
#endif
        if (!config.is_array()) {
            LOG(ERROR) << "Invalid config file";
            return false;
        }
        for (const auto& item : config) {
            std::string func_name = item.at("funcName").get<std::string>();
            if (!ValidateFuncName(func_name)) {
                LOG(ERROR) << "Invalid func_name: " << func_name;
                return false;
            }
            if (entires_by_func_name_.count(func_name) > 0) {
                LOG(ERROR) << "Duplicate func_name: " << func_name;
                return false;
            }
            int func_id = item.at("funcId").get<int>();
            if (!ValidateFuncId(func_id)) {
                LOG(ERROR) << "Invalid func_id: " << func_id;
                return false;
            }
            if (entries_by_func_id_.count(func_id) > 0) {
                LOG(ERROR) << "Duplicate func_id: " << func_id;
                return false;
            }
            auto entry = std::make_unique<Entry>();
            entry->func_name = func_name;
            entry->func_id = func_id;
            if (StartsWith(func_name, "grpc:")) {
                std::string_view service_name = StripPrefix(func_name, "grpc:");
                LOG(INFO) << "Load configuration for gRPC service " << service_name
                          << "[" << func_id << "]";
                const json& grpc_methods = item.at("grpcMethods");
                if (!grpc_methods.is_array()) {
                    LOG(ERROR) << "grpcMethods field is not array";
                    return false;
                }
                for (const auto& method : grpc_methods) {
                    std::string method_name = method.get<std::string>();
                    LOG(INFO) << "Register method " << method_name << " for gRPC service "
                              << service_name;
                    entry->grpc_methods.insert(method_name);
                }
            } else {
                LOG(INFO) << "Load configuration for function " << func_name
                          << "[" << func_id << "]";
            }
            entires_by_func_name_[func_name] = entry.get();
            entries_by_func_id_[func_id] = entry.get();
            entries_.push_back(std::move(entry));
        }
#ifndef __FAAS_NODE_ADDON
    } catch (const json::exception& e) {
        LOG(ERROR) << "Invalid config file: " << e.what();
        return false;
    }
#endif
    return true;
}

}  // namespace faas
