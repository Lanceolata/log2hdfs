// Copyright (c) 2017 Lanceolata

#include "loggerimp.h"
// #include "util/loggerimp.h"

#include <stdarg.h>     // include for va_list
#include <sys/time.h>   // include for timeval tm


namespace log2hdfs {

namespace util {

// ------------------------------------------------------------------
// Logger

LoggerPtr Logger::Create(const std::string& log_path, int log_max_length) {
    return SimpleLogger::Init(log_path, log_max_length);
}

// ------------------------------------------------------------------
// Log

static LoggerPtr logger_;
static int log_max_length_;

bool LogInit(LoggerPtr logger) {
  if (!logger) {
    return false;
  }
  logger_ = logger;
  log_max_length_ = logger->max_length();
  return true;
}

void Log(LogLevel level, const char *fmt, ...) {
  if (logger_) {
    va_list ap;
    std::unique_ptr<char[]> msg(new char[log_max_length_]);

    va_start(ap, fmt);
    vsnprintf(msg.get(), log_max_length_, fmt, ap);
    va_end(ap);

    logger_->Log(level, msg.get());
  }
}

// ------------------------------------------------------------------
// SimpleLogger

LoggerPtr SimpleLogger::Init(const std::string& log_path,
                             int max_length) {
  if (log_path.empty() || max_length < 1) {
    return nullptr;
  }

  FILE *fp;
  if ((fp = fopen(log_path.c_str(), "a")) == NULL) {
    return nullptr;
  }

  return LoggerPtr(new SimpleLogger(log_path, fp, max_length));
}

void SimpleLogger::Log(Level level, const char *msg) const {
  char buf[64];
  size_t off;
  struct timeval tv;
  struct tm tm;
  const char *levelstr;

  switch (level) {
    case kLogInfo:
      levelstr = "INFO";
      break;
    case kLogWarn:
      levelstr = "WARN";
      break;
    case kLogError:
      levelstr = "ERROR";
      break;
    default:
      levelstr = "UNKNOWN";
  }

    if (gettimeofday(&tv, NULL) != 0) return;
    if (localtime_r(&tv.tv_sec, &tm) == NULL) return;
    off = strftime(buf, sizeof(buf), "%Y-%M-%d %H:%M:%S.", &tm);
    snprintf(buf + off, sizeof(buf) - off, "%03d",
             static_cast<int>(tv.tv_usec / 1000));
    fprintf(fp_, "%s %s %s\n", buf, levelstr, msg);

    fflush(fp_);
}

void SimpleLogger::Error(const char *fmt, ...) const {
    va_list ap;
    std::unique_ptr<char[]> msg(new char[max_length_]);

    va_start(ap, fmt);
    vsnprintf(msg.get(), max_length_, fmt, ap);
    va_end(ap);

    Log(Level::kLogError, msg.get());
}

void SimpleLogger::Warn(const char *fmt, ...) const {
    va_list ap;
    std::unique_ptr<char[]> msg(new char[max_length_]);

    va_start(ap, fmt);
    vsnprintf(msg.get(), max_length_, fmt, ap);
    va_end(ap);

    Log(Level::kLogWarn, msg.get());
}

void SimpleLogger::Info(const char *fmt, ...) const {
    va_list ap;
    std::unique_ptr<char[]> msg(new char[max_length_]);

    va_start(ap, fmt);
    vsnprintf(msg.get(), max_length_, fmt, ap);
    va_end(ap);

    Log(Level::kLogInfo, msg.get());
}

}   // namespace util

}   // namespace log2hdfs
