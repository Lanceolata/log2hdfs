
#include <stdarg.h>
#include <sys/time.h>

//#include "log2hdfs/util/logger.h"
#include "logger.h"

namespace log2hdfs {

namespace util {

std::shared_ptr<Logger> SimpleLogger::Make(
        const std::string& log_path, const int log_max_length) {
    
  if (log_path.empty() || log_max_length < 1) {
    return nullptr;
  }

  FILE *fp;
  if ((fp = fopen(log_path.c_str(), "a")) == NULL) {
    return nullptr;
  }

  return std::shared_ptr<Logger>(new SimpleLogger(log_path, fp, 
                                                  log_max_length));
}

void SimpleLogger::write_log(Level level, const char *msg) const {
    char buf[64];
    size_t off;
    struct timeval tv;
    struct tm tm;
    //pid_t pid = getpid();
    const char *levelstr;

    switch (level) {
    case LOG_ERROR:
        levelstr = "ERROR";
        break;
    case LOG_WARNING:
        levelstr = "WARN";
        break;
    case LOG_INFO:
        levelstr = "INFO";
        break;
    default:
        levelstr = "UNKNOWN";
    }

    if (gettimeofday(&tv, NULL) != 0) return;
    if (localtime_r(&tv.tv_sec, &tm) == NULL) return;
    off = strftime(buf, sizeof(buf), "%d %b %H:%M:%S.", &tm);
    snprintf(buf + off, sizeof(buf) - off, "%03d", (int)tv.tv_usec / 1000);
    fprintf(fp_, "%d: %s %s %s\n", (int)pid_, buf, levelstr, msg);

    fflush(fp_);
}

void SimpleLogger::error(const char *fmt, ...) const {
    va_list ap;
    char msg[log_max_length_];

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);
    
    write_log(Level::LOG_ERROR, msg);
}

void SimpleLogger::warning(const char *fmt, ...) const {
    va_list ap;
    char msg[log_max_length_];

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    write_log(Level::LOG_WARNING, msg);
}

void SimpleLogger::info(const char *fmt, ...) const {
    va_list ap;
    char msg[log_max_length_];

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    write_log(Level::LOG_INFO, msg);
}

}   // namespace util

}   // namespace log2hdfs
