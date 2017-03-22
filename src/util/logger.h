// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_UTIL_LOGGER_H_
#define LOG2HDFS_UTIL_LOGGER_H_

#include <stdio.h>
#include <string>
#include <memory>

namespace log2hdfs {

class Logger {
 public:
  enum Level {
    kLogInfo,
    kLogWarn,
    kLogError
  };

  static std::shared_ptr<Logger> Init(const std::string &log_path,
                                      size_t max_length);

  Logger(const Logger &other) = delete;
  Logger &operator=(const Logger &other) = delete;

  ~Logger() {
    if (fp_ != NULL) {
      fclose(fp_);
      fp_ = NULL;
    }
  }

  size_t max_length() const {
    return max_length_;
  }

  void Log(Level level, const char *msg) const;

  void Error(const char *fmt, ...) const;
  void Warn(const char *fmt, ...) const;
  void Info(const char *fmt, ...) const;

 private:
  Logger(const std::string &log_path, FILE *fp, size_t max_length):
      log_path_(log_path), fp_(fp), max_length_(max_length) {}

  std::string log_path_;
  FILE *fp_;
  size_t max_length_;
};

typedef Logger::Level LogLevel;

extern bool LogInit(std::shared_ptr<Logger> logger);

extern void Log(LogLevel level, const char *fmt, ...);

}   // namespace log2hdfs

#endif  // LOG2HDFS_UTIL_LOGGER_H_
