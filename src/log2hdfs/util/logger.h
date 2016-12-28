#ifndef LOG2HDFS_UTIL_LOGGER_H
#define LOG2HDFS_UTIL_LOGGER_H

#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>

#include <string>
#include <memory>

namespace log2hdfs {

namespace util {

class Logger {
 public:

  enum Level {
    LOG_INFO = 0,
    LOG_WARNING = 1,
    LOG_ERROR = 2
  };

  virtual ~Logger() {}

  virtual void error(const char *fmt, ...) const = 0;
  virtual void warning(const char *fmt, ...) const = 0;
  virtual void info(const char *fmt, ...) const = 0;
};

class SimpleLogger : public Logger {
 public:

  // init SimpleLogger  
  static std::shared_ptr<Logger> Make(const std::string& log_path, 
                                      const int log_max_length = 1024);

  ~SimpleLogger() {
    if (fp_ == NULL) {
        fclose(fp_);
        fp_ = NULL;
    }
  }

  // implement
  void error(const char *fmt, ...) const;
  void warning(const char *fmt, ...) const;
  void info(const char *fmt, ...) const;

 private:

  // constructor  
  SimpleLogger(const std::string& log_path, FILE *fp, const int log_max_length):
      log_path_(log_path), fp_(fp), log_max_length_(log_max_length), 
      pid_(getpid()) {}

  void write_log(Level level, const char *msg) const;

  std::string log_path_;
  FILE *fp_;
  const int log_max_length_;
  pid_t pid_;
};

}   // namespace log2hdfs

}   // namespace util

#endif  // LOG2HDFS_UTIL_LOG_H
