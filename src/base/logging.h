#pragma once

#include <sstream>
#include <string>
#include <memory>
#include <utility>

#include "base/macro.h"

#define __ATTRIBUTE_NORETURN __attribute__((noreturn))

namespace faas {
namespace logging {

void Init(int level);

enum LogSeverity { INFO, WARNING, ERROR, FATAL };

template <typename T>
inline void MakeCheckOpValueString(std::ostream* os, const T& v) {
    (*os) << v;
}
template <>
void MakeCheckOpValueString(std::ostream* os, const char& v);
template <>
void MakeCheckOpValueString(std::ostream* os, const signed char& v);
template <>
void MakeCheckOpValueString(std::ostream* os, const unsigned char& v);
template <>
void MakeCheckOpValueString(std::ostream* os, const std::nullptr_t& p);

class CheckOpMessageBuilder {
public:
    explicit CheckOpMessageBuilder(const char* exprtext);
    ~CheckOpMessageBuilder();
    std::ostream* ForVar1() { return stream_; }
    std::ostream* ForVar2();
    std::string* NewString();
private:
    std::ostringstream* stream_;
};

template <typename T1, typename T2>
std::string* MakeCheckOpString(const T1& v1, const T2& v2, const char* exprtext) {
    CheckOpMessageBuilder comb(exprtext);
    MakeCheckOpValueString(comb.ForVar1(), v1);
    MakeCheckOpValueString(comb.ForVar2(), v2);
    return comb.NewString();
}

#define DEFINE_CHECK_OP_IMPL(name, op)                           \
    template <typename T1, typename T2>                          \
    inline std::string* name##Impl(const T1& v1, const T2& v2,   \
                                   const char* exprtext) {       \
        if (__FAAS_PREDICT_TRUE(v1 op v2)) return nullptr;       \
        return MakeCheckOpString(v1, v2, exprtext);              \
    }                                                            \
    inline std::string* name##Impl(int v1, int v2,               \
                                   const char* exprtext) {       \
        return name##Impl<int, int>(v1, v2, exprtext);           \
    }

DEFINE_CHECK_OP_IMPL(Check_EQ, ==)
DEFINE_CHECK_OP_IMPL(Check_NE, !=)
DEFINE_CHECK_OP_IMPL(Check_LE, <=)
DEFINE_CHECK_OP_IMPL(Check_LT, <)
DEFINE_CHECK_OP_IMPL(Check_GE, >=)
DEFINE_CHECK_OP_IMPL(Check_GT, >)
#undef DEFINE_CHECK_OP_IMPL

template <typename T>
inline const T& GetReferenceableValue(const T& t) {
    return t;
}
inline char GetReferenceableValue(char t) { return t; }
inline uint8_t GetReferenceableValue(uint8_t t) { return t; }
inline int8_t GetReferenceableValue(int8_t t) { return t; }
inline int16_t GetReferenceableValue(int16_t t) { return t; }
inline uint16_t GetReferenceableValue(uint16_t t) { return t; }
inline int32_t GetReferenceableValue(int32_t t) { return t; }
inline uint32_t GetReferenceableValue(uint32_t t) { return t; }
inline int64_t GetReferenceableValue(int64_t t) { return t; }
inline uint64_t GetReferenceableValue(uint64_t t) { return t; }

void set_vlog_level(int level);
int get_vlog_level();

class LogMessage {
public:
    LogMessage(const char* file, int line, LogSeverity severity = INFO,
               bool append_err_str = false);
    ~LogMessage();
    std::ostringstream& stream() { return stream_; }

protected:
    void SendToLog(const std::string& message_text);
    void AppendErrStrIfNecessary();

    LogSeverity severity_;
    std::ostringstream stream_;
    int preserved_errno_;
    bool append_err_str_;

private:
    void Init(const char* file, int line, LogSeverity severity);
    LogMessage(const LogMessage&) = delete;
    void operator=(const LogMessage&) = delete;
};

class LogMessageVoidify {
public:
    void operator&(const std::ostream&) {}
};

class LogMessageFatal : public LogMessage {
public:
    __ATTRIBUTE_NORETURN ~LogMessageFatal();
    LogMessageFatal(const char* file, int line, bool append_err_str = false)
        : LogMessage(file, line, FATAL, append_err_str) {}
    LogMessageFatal(const char* file, int line, const std::string& result);
};

template <typename T>
T CheckNotNull(const char* file, int line, const char* exprtext, T&& t) {
    if (__FAAS_PREDICT_FALSE(!t)) {
        LogMessageFatal(file, line, std::string(exprtext));
    }
    return std::forward<T>(t);
}

}  // namespace logging
}  // namespace faas

// Start public macro definitions

#define COMPACT_FAAS_LOG_INFO    faas::logging::LogMessage(__FILE__, __LINE__)
#define COMPACT_FAAS_LOG_WARNING faas::logging::LogMessage(__FILE__, __LINE__, faas::logging::WARNING)
#define COMPACT_FAAS_LOG_ERROR   faas::logging::LogMessage(__FILE__, __LINE__, faas::logging::ERROR)
#define COMPACT_FAAS_LOG_FATAL   faas::logging::LogMessageFatal(__FILE__, __LINE__)

#define LOG(severity) COMPACT_FAAS_LOG_##severity.stream()
#define LOG_IF(severity, condition) \
    !(condition) ? (void)0 : faas::logging::LogMessageVoidify() & LOG(severity)
#define VLOG(level) LOG_IF(INFO, __FAAS_PREDICT_FALSE((level) <= faas::logging::get_vlog_level()))
#define CHECK(condition) \
    LOG_IF(FATAL, __FAAS_PREDICT_FALSE(!(condition))) << "Check failed: " #condition " "

#define COMPACT_FAAS_PLOG_INFO    faas::logging::LogMessage(__FILE__, __LINE__, faas::logging::INFO, true)
#define COMPACT_FAAS_PLOG_WARNING faas::logging::LogMessage(__FILE__, __LINE__, faas::logging::WARNING, true)
#define COMPACT_FAAS_PLOG_ERROR   faas::logging::LogMessage(__FILE__, __LINE__, faas::logging::ERROR, true)
#define COMPACT_FAAS_PLOG_FATAL   faas::logging::LogMessageFatal(__FILE__, __LINE__, true)

#define PLOG(severity) COMPACT_FAAS_PLOG_##severity.stream()
#define PLOG_IF(severity, condition) \
    !(condition) ? (void)0 : faas::logging::LogMessageVoidify() & PLOG(severity)
#define PCHECK(condition) \
    PLOG_IF(FATAL, __FAAS_PREDICT_FALSE(!(condition))) << "Check failed: " #condition " "

#define CHECK_OP_LOG(name, op, val1, val2, log)            \
    while (auto _result = std::unique_ptr<std::string>(    \
           faas::logging::name##Impl(                      \
               faas::logging::GetReferenceableValue(val1), \
               faas::logging::GetReferenceableValue(val2), \
               #val1 " " #op " " #val2)))                  \
    log(__FILE__, __LINE__, *_result).stream()

#define CHECK_OP(name, op, val1, val2) \
    CHECK_OP_LOG(name, op, val1, val2, faas::logging::LogMessageFatal)

#define CHECK_EQ(val1, val2) CHECK_OP(Check_EQ, ==, val1, val2)
#define CHECK_NE(val1, val2) CHECK_OP(Check_NE, !=, val1, val2)
#define CHECK_LE(val1, val2) CHECK_OP(Check_LE, <=, val1, val2)
#define CHECK_LT(val1, val2) CHECK_OP(Check_LT, <, val1, val2)
#define CHECK_GE(val1, val2) CHECK_OP(Check_GE, >=, val1, val2)
#define CHECK_GT(val1, val2) CHECK_OP(Check_GT, >, val1, val2)

#define CHECK_NOTNULL(val) \
    faas::logging::CheckNotNull(__FILE__, __LINE__, "'" #val "' Must be non NULL", (val))

#if defined(NDEBUG) && !defined(DCHECK_ALWAYS_ON)
#define DCHECK_IS_ON() 0
#else
#define DCHECK_IS_ON() 1
#endif

#if DCHECK_IS_ON()

#define DLOG(severity)               LOG(severity)
#define DLOG_IF(severity, condition) LOG_IF(severity, condition)
#define DCHECK(condition)            CHECK(condition)
#define DCHECK_EQ(val1, val2)        CHECK_EQ(val1, val2)
#define DCHECK_NE(val1, val2)        CHECK_NE(val1, val2)
#define DCHECK_LE(val1, val2)        CHECK_LE(val1, val2)
#define DCHECK_LT(val1, val2)        CHECK_LT(val1, val2)
#define DCHECK_GE(val1, val2)        CHECK_GE(val1, val2)
#define DCHECK_GT(val1, val2)        CHECK_GT(val1, val2)
#define DCHECK_NOTNULL(val)          CHECK_NOTNULL(val)

#else  // DCHECK_IS_ON()

#define DLOG(severity)    \
    static_cast<void>(0), \
    true ? (void) 0 : faas::logging::LogMessageVoidify() & LOG(severity)

#define DLOG_IF(severity, condition) \
    static_cast<void>(0),            \
    (true || !(condition)) ? (void) 0 : faas::logging::LogMessageVoidify() & LOG(severity)

#define DCHECK(condition)     while (false) CHECK(condition)
#define DCHECK_EQ(val1, val2) while (false) CHECK_EQ(val1, val2)
#define DCHECK_NE(val1, val2) while (false) CHECK_NE(val1, val2)
#define DCHECK_LE(val1, val2) while (false) CHECK_LE(val1, val2)
#define DCHECK_LT(val1, val2) while (false) CHECK_LT(val1, val2)
#define DCHECK_GE(val1, val2) while (false) CHECK_GE(val1, val2)
#define DCHECK_GT(val1, val2) while (false) CHECK_GT(val1, val2)
#define DCHECK_NOTNULL(val)   val

#endif  // DCHECK_IS_ON()

#define DVLOG(level) DLOG_IF(INFO, __FAAS_PREDICT_FALSE((level) <= faas::logging::get_vlog_level()))

#undef __ATTRIBUTE_NORETURN
