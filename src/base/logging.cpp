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

namespace faas {
namespace logging {

int vlog_level = 0;
thread_local bool log_panic = false;

const char* GetBasename(const char* file_path) {
    const char* slash = strrchr(file_path, '/');
    return slash ? slash + 1 : file_path;
}

std::string StrError(int err) {
    return std::string(strerror(err));
}

void set_vlog_level(int level) { vlog_level = level; }
int get_vlog_level() { return vlog_level; }

void Init(int level) {
    set_vlog_level(level);
}

LogMessage::LogMessage(const char* file, int line) {
    Init(file, line, INFO);
}

LogMessage::LogMessage(const char* file, int line, LogSeverity severity) {
    Init(file, line, severity);
}

LogMessage::LogMessage(const char* file, int line, const std::string& result) {
    Init(file, line, FATAL);
    stream() << "Check failed: " << result << " ";
}

static constexpr const char* LogSeverityNames[4] = {
    "INFO", "WARN", "ERROR", "FATAL"
};

void LogMessage::Init(const char* file, int line, LogSeverity severity) {
    // Disallow recursive fatal messages.
    if (log_panic) {
        abort();
    }
    severity_ = severity;
    preserved_errno_ = errno;
    if (severity_ == FATAL) {
        log_panic = true;
    }
    const char *filename = GetBasename(file);
    // Write a prefix into the log message, including local date/time, severity
    // level, filename, and line number.
    struct timespec time_stamp;
    clock_gettime(CLOCK_REALTIME, &time_stamp);
    constexpr int kTimeMessageSize = 22;
    struct tm datetime;
    memset(&datetime, 0, sizeof(datetime));
    if (localtime_r(&time_stamp.tv_sec, &datetime)) {
        char buffer[kTimeMessageSize];
        strftime(buffer, kTimeMessageSize, "%Y-%m-%d %H:%M:%S ", &datetime);
        stream() << buffer;
    } else {
        // localtime_r returns error. Attach the errno message.
        stream() << "Failed to get time:" << strerror(errno) << "  ";
    }
    stream() << "[" << LogSeverityNames[severity_] << "] " << filename << ":" << line << "] ";
}

LogMessage::~LogMessage() {
    std::string message_text = stream_.str();
    SendToLog(message_text);
}

LogMessageFatal::~LogMessageFatal() {
    std::string message_text = stream_.str();
    SendToLog(message_text);
    // if FATAL occurs, abort enclave.
    if (severity_ == FATAL) {
        abort();
    }
    _exit(1);
}

ErrnoLogMessage::~ErrnoLogMessage() {
    stream() << ": " << StrError(preserved_errno_) << " ["
             << preserved_errno_ << "]";
    std::string message_text = stream_.str();
    SendToLog(message_text);
    // if FATAL occurs, abort enclave.
    if (severity_ == FATAL) {
        abort();
    }
    _exit(1);
}

void LogMessage::SendToLog(const std::string& message_text) {
    fprintf(stderr, "%s\n", message_text.c_str());
    fflush(stderr);
}

CheckOpMessageBuilder::CheckOpMessageBuilder(const char* exprtext)
    : stream_(new std::ostringstream) { *stream_ << exprtext << " ("; }

CheckOpMessageBuilder::~CheckOpMessageBuilder() { delete stream_; }

std::ostream* CheckOpMessageBuilder::ForVar2() {
    *stream_ << " vs. ";
    return stream_;
}

std::string* CheckOpMessageBuilder::NewString() {
    *stream_ << ")";
    return new std::string(stream_->str());
}

template <>
void MakeCheckOpValueString(std::ostream* os, const char& v) {
    if (v >= 32 && v <= 126) {
        (*os) << "'" << v << "'";
    } else {
        (*os) << "char value " << static_cast<int16_t>(v);
    }
}

template <>
void MakeCheckOpValueString(std::ostream* os, const signed char& v) {
    if (v >= 32 && v <= 126) {
      (*os) << "'" << v << "'";
    } else {
      (*os) << "signed char value " << static_cast<int16_t>(v);
    }
}

template <>
void MakeCheckOpValueString(std::ostream* os, const unsigned char& v) {
    if (v >= 32 && v <= 126) {
        (*os) << "'" << v << "'";
    } else {
        (*os) << "unsigned char value " << static_cast<uint16_t>(v);
    }
}

template <>
void MakeCheckOpValueString(std::ostream* os, const std::nullptr_t& v) {
    (*os) << "nullptr";
}

}  // namespace logging
}  // namespace faas
