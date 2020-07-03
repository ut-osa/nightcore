#pragma once

#include "base/common.h"
#include "common/protocol.h"

namespace faas {

class FuncConfig {
public:
    FuncConfig() {}
    ~FuncConfig() {}

    static constexpr int kMaxFuncId = (1 << protocol::kFuncIdBits) - 1;
    static constexpr int kMaxMethodId = (1 << protocol::kMethodIdBits) - 1;

    struct Entry {
        std::string func_name;
        int func_id;
        int min_workers;
        int max_workers;
        bool allow_http_get;
        bool qs_as_input;
        bool is_grpc_service;
        std::string grpc_service_name;
        std::vector<std::string> grpc_methods;
        std::unordered_map<std::string, int> grpc_method_ids;
    };

    bool Load(std::string_view json_contents);

    const Entry* find_by_func_name(std::string_view func_name) const {
        if (entires_by_func_name_.count(std::string(func_name)) > 0) {
            return entires_by_func_name_.at(std::string(func_name));
        } else {
            return nullptr;
        }
    }

    const Entry* find_by_func_id(int func_id) const {
        if (entries_by_func_id_.count(func_id) > 0) {
            return entries_by_func_id_.at(func_id);
        } else {
            return nullptr;
        }
    }

private:
    std::vector<std::unique_ptr<Entry>> entries_;
    std::unordered_map<std::string, Entry*> entires_by_func_name_;
    std::unordered_map<int, Entry*> entries_by_func_id_;

    static bool ValidateFuncId(int func_id);
    static bool ValidateFuncName(std::string_view func_name);

    DISALLOW_COPY_AND_ASSIGN(FuncConfig);
};

}  // namespace faas
