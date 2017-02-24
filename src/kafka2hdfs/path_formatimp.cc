// Copyright (c) 2017 Lanceolata

#include "kafka2hdfs/path_formatimp.h"
#include <time.h>
#include <vector>
#include <sstream>
#include <iomanip>
#include "kafka/kafka_message.h"
#include "util/string_utils.h"
#include "util/logger.h"

namespace log2hdfs {

namespace {

bool ArgsValid(const std::string &topic_name, const std::string &local_dir,
               const std::string &upload_dir,
               const std::string &hdfs_path_format, int interval) {
  if (topic_name.empty() || local_dir.empty() || upload_dir.empty() 
          || hdfs_path_format.empty() || interval < 60) {
    return false;
  }
  return true;
}

time_t AlignTimestamp(time_t ts, int interval) {
  
  return 0;
}

}   // namespace

// ------------------------------------------------------------------
// PathFormat

std::shared_ptr<PathFormat> PathFormat::Init(
    PathFormat::Type type, const std::string &topic_name,
    const std::string &local_dir, const std::string &upload_dir,
    const std::string &hdfs_path_format, int interval) {

  std::shared_ptr<PathFormat> res = nullptr;
  switch (type) {
    case kV6Log:
      res = V6PathFormat::Init(topic_name, local_dir, upload_dir,
                               hdfs_path_format, interval);
      break;
    case kEfLog:
      res = nullptr;
      break;
    default:
      Log(LogLevel::kLogWarn, "Unknown PathFormat type");
      break;
  }
  return res;
}

// ------------------------------------------------------------------
// PathFormatImp

Optional<std::string> PathFormatImp::BuildLocalPathFromMsg(
    const Message *msg) const {
  if (!msg) {
    Log(LogLevel::kLogWarn, "topic[%s] null Message pointer",
        topic_name_.c_str());
    return Optional<std::string>::Invalid();
  }

  time_t ts;
  std::string device;
  char *payload = static_cast<char *>(msg->Payload());
  bool res = ExtractInfoFromPayload(payload, &ts, &device);
  if (!res) {
    Log(LogLevel::kLogWarn, "ExtractInfoFromPayload failed topic[%s] "
        "payload[%s]", topic_name_.c_str(), payload);
    return Optional<std::string>::Invalid();
  }

  ts = AlignTimestamp(ts, interval_);

  char local_path[512];
  int n = snprintf(local_path, sizeof(local_path), "%s/%s_%ld_%s",
                   local_dir_.c_str(), topic_name_.c_str(),
                   ts, device.c_str());
  if (n <= 0) {
    Log(LogLevel::kLogError, "snprintf failed local_dir[%s] topic_name[%s] "
        "time_stamp[%ld] device_type[%s]", local_dir_.c_str(),
        topic_name_.c_str(), ts, device.c_str());
    return Optional<std::string>::Invalid();
  }

  return Optional<std::string>(local_path);
}

#define TIMESTAMP_SIZE 10

Optional<time_t> PathFormatImp::ExtractTimeStampFromFilename(
    const std::string &filename) const {
  if (filename.empty()) {
    Log(LogLevel::kLogWarn, "topic[%s] empty filename", topic_name_.c_str());
    return Optional<time_t>::Invalid();
  }
  
  if (!StartsWith(filename.c_str(), topic_name_.c_str())) {
    Log(LogLevel::kLogWarn, "Filename[%s] not starts with topicname[%s]",
        filename.c_str(), topic_name_.c_str());
    return Optional<time_t>::Invalid();
  }

  size_t index = topic_name_.size() + 1;
  size_t found = filename.find("_", index);
  if (found == std::string::npos) {
    Log(LogLevel::kLogWarn, "Invalid filename[%s]", filename.c_str());
    return Optional<time_t>::Invalid();
  }

  if (found - index != TIMESTAMP_SIZE) {
    Log(LogLevel::kLogWarn, "Invalid filename[%s]", filename.c_str());
    return Optional<time_t>::Invalid();
  }

  time_t ts = StringToTs(filename.substr(index, TIMESTAMP_SIZE));
  return Optional<time_t>(ts);
}

Optional<std::string> PathFormatImp::BuildHdfsPathFromLocalpath(
    const std::string &filepath) const {
  if (filepath.empty()) {
    Log(LogLevel::kLogWarn, "BuildHdfsPathFromLocalpath empty filepath");
    return Optional<std::string>::Invalid();
  }

  if (!StartsWith(filepath.c_str(), upload_dir_.c_str())) {
    Log(LogLevel::kLogWarn, "Invalid filepath[%s] upload_dir[%s]",
        filepath.c_str(), upload_dir_.c_str());
    return Optional<std::string>::Invalid();
  }

  size_t start = filepath.find_last_of("/");
  if (start == std::string::npos) {
    Log(LogLevel::kLogWarn, "Invalid filepath[%s]", filepath.c_str());
    return Optional<std::string>::Invalid();
  }

  if (!StartsWith(filepath.c_str() + start + 1, topic_name_.c_str())) {
    Log(LogLevel::kLogWarn, "Invalid filepath[%s]", filepath.c_str());
    return Optional<std::string>::Invalid();
  }

  start += topic_name_.size() + 2;
  size_t end = filepath.find_first_of(".");
  if (end == std::string::npos) {
    Log(LogLevel::kLogWarn, "Invalid filepath[%s]", filepath.c_str());
    return Optional<std::string>::Invalid();
  }

  std::vector<std::string> vec = SplitString(
      filepath.substr(start, end - start), "_",
      WhitespaceHandling::kTrimWhitespace, SplitResult::kSplitAll);

  if (vec.empty() || vec.size() != 3 || vec[0].size() != TIMESTAMP_SIZE
          || vec[2].size() != TIMESTAMP_SIZE) {
    Log(LogLevel::kLogWarn, "Invalid filepath[%s]", filepath.c_str());
    return Optional<std::string>::Invalid();
  }

  time_t ts = StringToTs(vec[0]);
  struct tm timeinfo;
  if (localtime_r(&ts, &timeinfo) == NULL) {
    Log(LogLevel::kLogWarn, "localtime_r failed topic[%s] filepath[%s]",
        topic_name_.c_str(), filepath.c_str());
    return Optional<std::string>::Invalid();
  }
  
  std::ostringstream os;
  std::string::const_iterator it;
  for (it = hdfs_path_format_.begin(); it != hdfs_path_format_.end(); ++it) {
    if (*it == '%') {
      ++it;
      if (it == hdfs_path_format_.end()) {
        continue;
      }
      switch (*it) {
        case 'Y':
          os << 1900 + timeinfo.tm_year;
          break;
        case 'm':
          os << std::setfill('0') << std::setw(2) << timeinfo.tm_mon;
          break;
        case 'd':
          os << std::setfill('0') << std::setw(2) << timeinfo.tm_mday;
          break;
        case 'H':
          os << std::setfill('0') << std::setw(2) << timeinfo.tm_hour;
          break;
        case 'M':
          os << std::setfill('0') << std::setw(2) << timeinfo.tm_min;
          break;
        case 't':
          os << topic_name_;
          break;
        case 'T':
          os << vec[2];
          break;
        case 'D':
          os << vec[1];
          break;
        default:
          Log(LogLevel::kLogWarn,"Unknown format flags");
          return Optional<std::string>::Invalid();
      }
    } else {
      os << *it;
    }
  }
  return Optional<std::string>(os.str());
}

// ------------------------------------------------------------------
// V6PathFormat

std::shared_ptr<PathFormat> V6PathFormat::Init(
    const std::string &topic_name, const std::string &local_dir,
    const std::string &hdfs_path_format) {
  bool valid = ArgsValid(topic_name, local_dir, hdfs_path_format);
  if (!valid) {
    return nullptr;
  }
  return std::shared_ptr<PathFormat>(new V6PathFormat(topic_name, local_dir,
                                                      hdfs_path_format));
}

std::unique_ptr<V6PathFormat::FormatInfo> V6PathFormat::ExtractFromPayload(
    const char *payload) const {
  
}

// ------------------------------------------------------------------
// EfPathFormat

}   // namespace log2hdfs
