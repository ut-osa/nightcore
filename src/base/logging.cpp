#define __FAAS_USED_IN_BINDING
#include "base/logging.h"

#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <signal.h>

#include <fmt/format.h>
#include <fmt/core.h>

#ifdef __FAAS_SRC

#include <absl/synchronization/mutex.h>
#include <absl/synchronization/notification.h>
#include "base/thread.h"

#else  // __FAAS_SRC

#include <sys/syscall.h>

static pid_t __gettid() {
    return syscall(SYS_gettid);
}

#endif  // __FAAS_SRC

#define __PREDICT_FALSE(x) __builtin_expect(x, 0)
#define __ATTRIBUTE_UNUSED __attribute__((unused))

namespace faas {
namespace logging {

namespace {

int vlog_level = 0;

const char* GetBasename(const char* file_path) {
    const char* slash = strrchr(file_path, '/');
    return slash ? slash + 1 : file_path;
}

void Abort() {
#ifdef __FAAS_SRC
    raise(SIGABRT);
#else
    abort();
#endif
}

}

void set_vlog_level(int level) { vlog_level = level; }
int get_vlog_level() { return vlog_level; }

void Init(int level) {
    set_vlog_level(level);
}

__ATTRIBUTE_UNUSED static constexpr const char* kLogSeverityNames[4] = {
    "INFO", "WARN", "ERROR", "FATAL"
};

static constexpr const char* kLogSeverityShortNames = "IWEF";

LogMessage::LogMessage(const char* file, int line, LogSeverity severity, bool append_err_str) {
    severity_ = severity;
    preserved_errno_ = errno;
    append_err_str_ = append_err_str;
    const char* filename = GetBasename(file);
    // Write a prefix into the log message, including local date/time, severity
    // level, filename, and line number.
    struct timespec time_stamp;
    if (__PREDICT_FALSE(clock_gettime(CLOCK_REALTIME, &time_stamp) != 0)) {
        perror("clock_gettime failed");
        abort();
    }
    constexpr int kBufferSize = 1 /* severity char */ + 14 /* datetime */ + 6 /* usec */
                              + 1 /* space */ + 8 /* thread name */ + 1 /* \0 */;
    char buffer[kBufferSize];
    buffer[0] = kLogSeverityShortNames[severity_];
    struct tm datetime;
    memset(&datetime, 0, sizeof(datetime));
    if (__PREDICT_FALSE(localtime_r(&time_stamp.tv_sec, &datetime) == NULL)) {
        perror("localtime_r failed");
        abort();
    }
    strftime(buffer+1, 14, "%m%d %H:%M:%S", &datetime);
    buffer[14] = '.';
    sprintf(buffer+15, "%06d", static_cast<int>(time_stamp.tv_nsec / 1000));
#ifdef __FAAS_SRC
    sprintf(buffer+21, " %-8.8s", base::Thread::current()->name());
#else
    sprintf(buffer+21, " %d", static_cast<int>(__gettid()));
#endif
    stream() << fmt::format("{} {}:{}] ", buffer, filename, line);
}

LogMessage::~LogMessage() {
    AppendErrStrIfNecessary();
    std::string message_text = stream_.str();
    SendToLog(message_text);
    if (severity_ == FATAL) {
        Abort();
    }
}

LogMessageFatal::LogMessageFatal(const char* file, int line, const std::string& result)
    : LogMessage(file, line, FATAL) { stream() << fmt::format("Check failed: {} ", result); }

LogMessageFatal::~LogMessageFatal() {
    AppendErrStrIfNecessary();
    std::string message_text = stream_.str();
    SendToLog(message_text);
    Abort();
    exit(EXIT_FAILURE);
}

#ifdef __FAAS_SRC
absl::Mutex stderr_mu;
#endif

void LogMessage::SendToLog(const std::string& message_text) {
#ifdef __FAAS_SRC
    absl::MutexLock lk(&stderr_mu);
#endif
    fprintf(stderr, "%s\n", message_text.c_str());
    fflush(stderr);
}

void LogMessage::AppendErrStrIfNecessary() {
    if (append_err_str_) {
        stream() << fmt::format(": {} [{}]", strerror(append_err_str_), preserved_errno_);
    }
}

CheckOpMessageBuilder::CheckOpMessageBuilder(const char* exprtext)
    : stream_(new std::ostringstream) { *stream_ << exprtext << " ("; }

CheckOpMessageBuilder::~CheckOpMessageBuilder() { delete stream_; }

std::ostream* CheckOpMessageBuilder::ForVar2() {
    *stream_ << " vs ";
    return stream_;
}

std::string* CheckOpMessageBuilder::NewString() {
    *stream_ << ")";
    return new std::string(stream_->str());
}

template <>
void MakeCheckOpValueString(std::ostream* os, const char& v) {
    if (v >= 32 && v <= 126) {
        (*os) << fmt::format("'{}'", v);
    } else {
        (*os) << fmt::format("char value {}", static_cast<int16_t>(v));
    }
}

template <>
void MakeCheckOpValueString(std::ostream* os, const signed char& v) {
    if (v >= 32 && v <= 126) {
      (*os) << fmt::format("'{}'", static_cast<char>(v));
    } else {
      (*os) << fmt::format("signed char value {}", static_cast<int16_t>(v));
    }
}

template <>
void MakeCheckOpValueString(std::ostream* os, const unsigned char& v) {
    if (v >= 32 && v <= 126) {
        (*os) << fmt::format("'{}'", static_cast<char>(v));
    } else {
        (*os) << fmt::format("unsigned value {}", static_cast<int16_t>(v));
    }
}

template <>
void MakeCheckOpValueString(std::ostream* os, const std::nullptr_t& v) {
    (*os) << "nullptr";
}

}  // namespace logging
}  // namespace faas
