// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_UTIL_LOGGER_H
#define LOG2HDFS_UTIL_LOGGER_H

#include <string>
#include <memory>

namespace log2hdfs {

namespace util {

class Logger;
typedef std::shared_ptr<Logger> LoggerPtr;

class Logger {
 public:
  enum Level {
    kLogInfo = 0,
    kLogWarn = 1,
    kLogError = 2
  };

  static LoggerPtr  Create(const std::string& log_path,
                           int max_length = 2048);
  virtual ~Logger() {}

  virtual int max_length() const = 0;

  virtual void Log(Level level, const char *msg) const = 0;

  virtual void Error(const char *fmt, ...) const = 0;
  virtual void Warn(const char *fmt, ...) const = 0;
  virtual void Info(const char *fmt, ...) const = 0;
};

typedef Logger::Level LogLevel;

extern bool LogInit(LoggerPtr logger);

extern void Log(LogLevel level, const char *fmt, ...);

}   // namespace util

}   // namespace log2hdfs

#endif  // LOG2HDFS_UTIL_LOGGER_H
