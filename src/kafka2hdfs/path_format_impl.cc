// Copyright (c) 2017 Lanceolata

#include "kafka2hdfs/path_format_impl.h"
#include <stdlib.h>
#include <vector>
#include <sstream>
#include <iomanip>
#include "kafka2hdfs/kafka2hdfs_topic_conf.h"
#include "kafka/kafka_message.h"
#include "util/system_utils.h"
#include "util/string_utils.h"
#include "util/logger.h"

namespace log2hdfs {

namespace {

bool CheckArgument(std::shared_ptr<LogFormat> log_format,
                   std::shared_ptr<Kafka2hdfsTopicConf> conf) {
  if (!log_format || !log_format.get()) {
    return false;
  }

  if (!conf || !conf.get()) {
    return false;
  }
  return true;
}

time_t AlignTimestamp(time_t ts, int interval) {
  time_t remainder = ts % (interval * 60);
  return ts + interval * 60 - remainder - 2;
}

}   // namespace

// ------------------------------------------------------------------
// PathFormat

Optional<PathFormat::Type> PathFormat::GetTypeFromString(
    const std::string &type) {
  if (type == "normal") {
    return Optional<PathFormat::Type>(PathFormat::kNormal);
  } else if (type == "delay") {
    return Optional<PathFormat::Type>(PathFormat::kDelay);
  } else {
    return Optional<PathFormat::Type>::Invalid();
  }
}

std::shared_ptr<PathFormat> PathFormat::Init(
    PathFormat::Type type,
    std::shared_ptr<LogFormat> log_format,
    std::shared_ptr<Kafka2hdfsTopicConf> conf) {
  std::shared_ptr<PathFormat> res;
  switch (type) {
    case kNormal:
      res = NormalFormatImpl::Init(log_format, conf);
      break;
    case kDelay:
      res = nullptr;
      break;
  }
  return res;
}

// ------------------------------------------------------------------
// NormalFormatImpl

std::shared_ptr<PathFormat> NormalFormatImpl::Init(
    std::shared_ptr<LogFormat> log_format,
    std::shared_ptr<Kafka2hdfsTopicConf> conf) {
  if (!CheckArgument(log_format, conf)) {
    return nullptr;
  }
  return std::make_shared<NormalFormatImpl>(log_format, conf);
}

Optional<std::string> NormalFormatImpl::BuildLocalPathFromMsg(
    const KafkaMessage *msg) const {
  if (!msg) {
    Log(LogLevel::kLogError, "section[%s] null Message pointer",
        conf_->section_name()->c_str());
    return Optional<std::string>::Invalid();
  }

  time_t ts;
  std::string device, type;
  char *payload = static_cast<char *>(msg->Payload());
  bool res = log_format_->ExtractInfoFromPayload(payload, &ts, &device, &type);
  if (!res) {
    Log(LogLevel::kLogWarn, "ExtractInfoFromPayload failed section[%s] "
        "topic[%s] payload[%s]", conf_->section_name()->c_str(),
        msg->TopicName().c_str(), payload);
    return Optional<std::string>::Invalid();
  }

  ts = AlignTimestamp(ts, conf_->consume_interval());
  struct tm timeinfo;
  if (localtime_r(&ts, &timeinfo) == NULL) {
    Log(LogLevel::kLogWarn, "localtime_r failed section[%s] topic[%s] "
        "payload[%s]", conf_->section_name()->c_str(),
        msg->TopicName().c_str(), payload);
    return Optional<std::string>::Invalid();
  }

  char local_path[512];
  int n = snprintf(local_path, sizeof(local_path), "%s/%s_%d%02d%02d"
                   "%02d%02d%02d_%s_%s", conf_->consume_dir()->c_str(),
                   conf_->section_name()->c_str(), timeinfo.tm_year + 1900,
                   timeinfo.tm_mon + 1, timeinfo.tm_mday, timeinfo.tm_hour,
                   timeinfo.tm_min, timeinfo.tm_sec, device.c_str(),
                   type.c_str());
  if (n <= 0) {
    Log(LogLevel::kLogError, "snprintf failed consume_dir[%s] section[%s] "
        "YmdH[%d%02d%02d%02d%02d%02d] device[%s] type[%s]",
        conf_->consume_dir()->c_str(), conf_->section_name()->c_str(),
        timeinfo.tm_year + 1900, timeinfo.tm_mon + 1, timeinfo.tm_mday,
        timeinfo.tm_hour, timeinfo.tm_min, timeinfo.tm_sec, device.c_str(),
        type.c_str());
    return Optional<std::string>::Invalid();
  }
  return Optional<std::string>(local_path);
}

#define YMDHMS_FORMAT "%Y%m%d%H%M%S"
#define YMDHMS_SIZE 14

bool NormalFormatImpl::WriteFinished(const std::string &filepath) const {
  std::shared_ptr<std::string> section = conf_->section_name();
  if (filepath.empty()) {
    Log(LogLevel::kLogWarn, "WriteFinished failed section[%s] with empty"
        " filename", section->c_str());
    return false;
  }
  
  Optional<std::string> fn = BaseName(filepath.c_str());
  if (!fn.valid()) {
    Log(LogLevel::kLogWarn, "BaseName[%s] failed", filepath.c_str());
    return false;
  }

  const std::string &filename = fn.value();
  if (!StartsWith(filename.c_str(), section->c_str())) {
    Log(LogLevel::kLogWarn, "Filename[%s] not starts with section[%s]",
        filename.c_str(), conf_->section_name()->c_str());
    return false;
  }

  size_t start = section->size() + 1;
  size_t end = filename.find("_", start);
  if (end == std::string::npos) {
    Log(LogLevel::kLogWarn, "Invalid filename[%s]", filename.c_str());
    return false;
  }
  if (end - start != YMDHMS_SIZE) {
    Log(LogLevel::kLogWarn, "Invalid filename[%s]", filename.c_str());
    return false;
  }

  std::string time_str = filename.substr(start, YMDHMS_SIZE);
  Optional<time_t> ts = StrToTs(time_str, YMDHMS_FORMAT);
  if (!ts.valid()) {
    Log(LogLevel::kLogWarn, "StrToTs[%s] failed", filename.c_str());
    return false;
  }

  Optional<off_t> filesize = FileSize(filepath.c_str());
  if (!filesize.valid()) {
    Log(LogLevel::kLogWarn, "FileSize[%s] failed", filepath.c_str());
    return false;
  }

  if (filesize.value() > conf_->consume_maxsize()) {
    return true;
  }

  if (ts.value() + 30 < time(NULL)) {
    Optional<time_t> mtime = FileMtime(filepath.c_str());
    if (!mtime.valid()) {
      Log(LogLevel::kLogWarn, "FileMtime[%s] failed", filepath.c_str());
      return false;
    }
    if (time(NULL) - mtime.value() > conf_->consume_complete_interval()) {
      return true;
    }
  }
  return false;
}

#define TIMESTAMP_SIZE 10

Optional<std::string> NormalFormatImpl::BuildHdfsPathFromLocalpath(
    const std::string &filepath) const {
  std::shared_ptr<std::string> section = conf_->section_name();
  if (filepath.empty()) {
    Log(LogLevel::kLogWarn, "section[%s] BuildHdfsPathFromLocalpath "
        "empty filepath", section->c_str());
    return Optional<std::string>::Invalid();
  }

  std::shared_ptr<std::string> upload_dir = conf_->upload_dir();
  if (!StartsWith(filepath.c_str(), upload_dir->c_str())) {
    Log(LogLevel::kLogWarn, "Invalid filepath[%s] upload_dir[%s]",
        filepath.c_str(), upload_dir->c_str());
    return Optional<std::string>::Invalid();
  }

  Optional<std::string> fn = BaseName(filepath.c_str());
  if (!fn.valid()) {
    Log(LogLevel::kLogWarn, "BaseName[%s] failed", filepath.c_str());
    return Optional<std::string>::Invalid();
  }

  const std::string &filename = fn.value();
  if (!StartsWith(filename.c_str(), section->c_str())) {
    Log(LogLevel::kLogWarn, "Invalid filepath[%s]", filepath.c_str());
    return Optional<std::string>::Invalid();
  }

  size_t start = section->size() + 1;
  size_t end = filename.find(".", start);
  if (end == std::string::npos) {
    Log(LogLevel::kLogWarn, "Invalid filepath[%s]", filepath.c_str());
    return Optional<std::string>::Invalid();
  }

  std::vector<std::string> vec = SplitString(
      filepath.substr(start, end - start), "_",
      WhitespaceHandling::kTrimWhitespace, SplitResult::kSplitAll);
  if (vec.empty() || vec.size() != 4 || vec[0].size() != YMDHMS_SIZE
          || vec[3].size() != TIMESTAMP_SIZE) {
    Log(LogLevel::kLogWarn, "Invalid filepath[%s]", filepath.c_str());
    return Optional<std::string>::Invalid();
  }

  struct tm timeinfo;
  if (strptime(vec[0].c_str(), YMDHMS_FORMAT, &timeinfo) == NULL) {
    Log(LogLevel::kLogWarn, "strptime failed filepath[%s]",
        filepath.c_str());
    return Optional<std::string>::Invalid();
  }

  std::shared_ptr<std::string> hdfs_path_format = conf_->hdfs_path_format();
  std::ostringstream os;
  std::string::const_iterator it;
  std::string::const_iterator it_end = hdfs_path_format->end();
  for (it = hdfs_path_format->begin(); it != it_end; ++it) {
    if (*it == '%') {
      ++it;
      if (it == it_end) {
        Log(LogLevel::kLogWarn, "Invalid hdfs_path_format[%s]",
            hdfs_path_format->c_str());
        return Optional<std::string>::Invalid();
      }
      switch (*it) {
        case 'Y':
          os << 1900 + timeinfo.tm_year;
          break;
        case 'm':
          os << std::setfill('0') << std::setw(2) << timeinfo.tm_mon + 1;
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
        case 'S':
          os << std::setfill('0') << std::setw(2) << timeinfo.tm_sec;
          break;
        case 's':
          os << *section;
          break;
        case 'D':
          os << vec[1];
          break;
        case 'T':
          os << vec[2];
          break;
        case 't':
          os << vec[3];
          break;
        default:
          Log(LogLevel::kLogWarn, "Unknown format flags");
          return Optional<std::string>::Invalid();
      }
    } else {
      os << *it;
    }
  }
  return Optional<std::string>(os.str());
}

// ------------------------------------------------------------------
// DelayPathFormat

}   // namespace log2hdfs
