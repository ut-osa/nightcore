#include "common/func_config.h"

#include "utils/fs.h"

#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace faas {

constexpr int FuncConfig::kMaxFuncId;

bool FuncConfig::ValidateFuncId(int func_id) {
    return 0 < func_id && func_id <= kMaxFuncId;
}

bool FuncConfig::ValidateFuncName(absl::string_view func_name) {
    for (const char& ch : func_name) {
        if (!(('0' <= ch && ch <= '9') ||
              ('a' <= ch && ch <= 'z') ||
              ('A' <= ch && ch <= 'Z'))) {
            return false;
        }
    }
    return true;
}

bool FuncConfig::Load(absl::string_view json_path) {
    std::string json_contents;
    if (!fs_utils::ReadContents(json_path, &json_contents)) {
        LOG(ERROR) << "Failed to read from file " << json_path;
        return false;
    }
    json config;
    try {
        config = json::parse(json_contents);
    } catch (const json::parse_error& e) {
        LOG(ERROR) << "Failed to parse json: " << e.what();
        return false;
    }
    try {
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
            if (entires_by_func_name_.contains(func_name)) {
                LOG(ERROR) << "Duplicate func_name: " << func_name;
                return false;
            }
            int func_id = item.at("funcId").get<int>();
            if (!ValidateFuncId(func_id)) {
                LOG(ERROR) << "Invalid func_id: " << func_id;
                return false;
            }
            if (entries_by_func_id_.contains(func_id)) {
                LOG(ERROR) << "Duplicate func_id: " << func_id;
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
    } catch (const json::exception& e) {
        LOG(ERROR) << "Invalid config file: " << e.what();
        return false;
    }
    return true;
}

}  // namespace faas
