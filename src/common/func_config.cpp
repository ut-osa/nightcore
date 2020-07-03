#define __FAAS_USED_IN_BINDING
#include "common/func_config.h"

#include "utils/fs.h"

#include <nlohmann/json.hpp>
using json = nlohmann::json;

namespace faas {

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

bool FuncConfig::Load(std::string_view json_contents) {
    json config;
#ifndef __FAAS_CXX_NO_EXCEPTIONS
    try {
#endif
        config = json::parse(json_contents);
#ifndef __FAAS_CXX_NO_EXCEPTIONS
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
            entry->min_workers = -1;
            entry->max_workers = -1;
            if (item.contains("minWorkers")) {
                entry->min_workers = item.at("minWorkers").get<int>();
            }
            if (item.contains("maxWorkers")) {
                entry->max_workers = item.at("maxWorkers").get<int>();
            }
            entry->allow_http_get = false;
            entry->qs_as_input = false;
            entry->is_grpc_service = false;
            if (StartsWith(func_name, "grpc:")) {
                std::string_view service_name = StripPrefix(func_name, "grpc:");
                entry->is_grpc_service = true;
                entry->grpc_service_name = std::string(service_name);
                LOG(INFO) << "Load configuration for gRPC service " << service_name
                          << "[" << func_id << "]";
                const json& grpc_methods = item.at("grpcMethods");
                if (!grpc_methods.is_array()) {
                    LOG(ERROR) << "grpcMethods field is not array";
                    return false;
                }
                for (const auto& method : grpc_methods) {
                    int method_id = entry->grpc_methods.size();
                    if (method_id > kMaxMethodId) {
                        LOG(ERROR) << "More than " << kMaxMethodId << " methods for gRPC service "
                                   << service_name;
                        return false;
                    }
                    std::string method_name = method.get<std::string>();
                    if (entry->grpc_method_ids.count(method_name) > 0) {
                        LOG(ERROR) << "Duplicate method " << method_name << " for gRPC service "
                                   << service_name;
                        return false;
                    } else {
                        LOG(INFO) << "Register method " << method_name << " for gRPC service "
                                  << service_name;
                        entry->grpc_methods.push_back(method_name);
                        entry->grpc_method_ids[method_name] = method_id;
                    }
                }
            } else {
                LOG(INFO) << "Load configuration for function " << func_name
                          << "[" << func_id << "]";
                if (item.contains("allowHttpGet") && item.at("allowHttpGet").get<bool>()) {
                    LOG(INFO) << "Allow HTTP GET enabled for " << func_name;
                    entry->allow_http_get = true;
                }
                if (item.contains("qsAsInput") && item.at("qsAsInput").get<bool>()) {
                    LOG(INFO) << "Query string used as input for " << func_name;
                    entry->qs_as_input = true;
                }
            }
            entires_by_func_name_[func_name] = entry.get();
            entries_by_func_id_[func_id] = entry.get();
            entries_.push_back(std::move(entry));
        }
#ifndef __FAAS_CXX_NO_EXCEPTIONS
    } catch (const json::exception& e) {
        LOG(ERROR) << "Invalid config file: " << e.what();
        return false;
    }
#endif
    return true;
}

}  // namespace faas
