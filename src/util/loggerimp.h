// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_UTIL_LOGGERIMP_H
#define LOG2HDFS_UTIL_LOGGERIMP_H

#include <stdio.h>      // include for fclose
#include <sys/types.h>  // include for getpid
#include <unistd.h>     // include for getpid
#include <string>
#include <memory>
// #include "util/logger.h"
#include "logger.h"

namespace log2hdfs {

namespace util {

class SimpleLogger : public Logger {
 public:
  // init SimpleLogger
  static LoggerPtr Init(const std::string& log_path, int max_length);

  ~SimpleLogger() {
    if (fp_ == NULL) {
        fclose(fp_);
        fp_ = NULL;
    }
  }

  SimpleLogger(const SimpleLogger& logger) = delete;
  SimpleLogger& operator=(const SimpleLogger& logger) = delete;

  int max_length() const {
    return max_length_;
  }
  void Log(Level level, const char *msg) const;

  // implement
  void Error(const char *fmt, ...) const;
  void Warn(const char *fmt, ...) const;
  void Info(const char *fmt, ...) const;

 private:
  // constructor
  SimpleLogger(const std::string& log_path, FILE *fp, int max_length):
      log_path_(log_path), fp_(fp), max_length_(max_length) {}

  std::string log_path_;
  FILE *fp_;
  int max_length_;
};

}   // namespace util

}   // namespace log2hdfs

#endif  // LOG2HDFS_UTIL_LOGGERIMP_H
