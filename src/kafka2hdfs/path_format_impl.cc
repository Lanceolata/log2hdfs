// Copyright (c) 2017 Lanceolata

#include "kafka2hdfs/path_format_impl.h"
#include <map>
#include <iomanip>
#include <sstream>
#include "kafka/kafka_message.h"
#include "kafka2hdfs/log_format.h"
#include "kafka2hdfs/topic_conf.h"
#include "util/system_utils.h"
#include "util/string_utils.h"
#include "easylogging++.h"

namespace log2hdfs {

namespace {

time_t AlignTimestamp(time_t ts, int interval) {
  time_t remainder = ts % interval;
  return ts + interval - remainder - 2;
}

}   // namespace

// ------------------------------------------------------------------
// PathFormat

Optional<PathFormat::Type> PathFormat::ParseType(const std::string &type) {
  if (type == "normal") {
    return Optional<PathFormat::Type>(kNormal);
  } else if (type == "delay") {
    return Optional<PathFormat::Type>(kDelay);
  } else {
    return Optional<PathFormat::Type>::Invalid();
  }
}

std::shared_ptr<PathFormat> PathFormat::Init(
    PathFormat::Type type, std::shared_ptr<TopicConf> conf) {
  std::shared_ptr<PathFormat> res;
  switch (type) {
    case kNormal:
      res = NormalPathFormat::Init(std::move(conf));
      break;
    case kDelay:
      res = nullptr;
      break;
    default:
      res = nullptr;
  }
  return res;
}

// ------------------------------------------------------------------
// NormalPathFormat

std::shared_ptr<NormalPathFormat> NormalPathFormat::Init(
    std::shared_ptr<TopicConf> conf) {
  if (!conf) {
    LOG(WARNING) << "NormalPathFormat Init invalid parameters";
    return nullptr;
  }

  std::string section = conf->section();
  if (section.empty()) {
    LOG(WARNING) << "NormalPathFormat Init invalid section";
    return nullptr;
  }

  std::unique_ptr<LogFormat> format = LogFormat::Init(conf->log_format());
  if (!format) {
    LOG(WARNING) << "NormalPathFormat Init LogFormat Init failed";
    return nullptr;
  }

  return std::make_shared<NormalPathFormat>(section,
             std::move(format), std::move(conf));
}

bool NormalPathFormat::BuildLocalFileName(
    const KafkaMessage& msg, std::string* name) const {
  if (!name) {
    LOG(WARNING) << "NormalPathFormat BuildLocalFileName invalid parameters";
    return false;
  }

  std::string key;
  time_t ts;
  const char *payload = static_cast<char *>(msg.Payload());
  size_t len = msg.Len();
  if (!format_->ExtractKeyAndTs(payload, len, &key, &ts)) {
    LOG(WARNING) << "NormalPathFormat BuildLocalFileName ExtractKeyAndTs"
                 << " failed";
    return false;
  }

  int consume_interval = conf_->consume_interval();
  time_t align_ts = AlignTimestamp(ts, consume_interval);
  struct tm timeinfo;
  if (localtime_r(&align_ts, &timeinfo) == NULL) {
    LOG(WARNING) << "NormalPathFormat BuildLocalFileName localtime_r["
                 << align_ts << "] failed";
    return false;
  }

  char local_path[512];
  int n = snprintf(local_path, sizeof(local_path), "%s.%s.%d%02d%02d"
                   "%02d%02d%02d", section_.c_str(), key.c_str(),
                   timeinfo.tm_year + 1900, timeinfo.tm_mon + 1,
                   timeinfo.tm_mday, timeinfo.tm_hour, timeinfo.tm_min,
                   timeinfo.tm_sec);
  if (n < 0) {
    LOG(ERROR) << "NormalPathFormat BuildLocalFileName snprintf failed";
    return false;
  }
  (*name).assign(local_path);
  return true;
}

bool NormalPathFormat::WriteFinished(const std::string& filepath) const {
  if (filepath.empty()) {
    LOG(WARNING) << "NormalPathFormat WriteFinished invalid parameters";
    return false;
  }

  if (!IsFile(filepath)) {
    LOG(WARNING) << "NormalPathFormat WriteFinished IsFile["
                 << filepath << "] failed";
    return false;
  }

  off_t maxsize = conf_->complete_maxsize();
  off_t file_size = FileSize(filepath);
  if (file_size < 0) {
    LOG(WARNING) << "NormalPathFormat WriteFinished FileSize["
                 << filepath << "] failed";
    return false;
  }

  if (file_size >= maxsize)
    return true;

  int interval = conf_->complete_interval();
  time_t file_ts = FileMtime(filepath);
  if (file_ts < 0) {
    LOG(WARNING) << "NormalPathFormat WriteFinished FileMtime["
                 << filepath << "] failed";
  }
  
  if (time(NULL) - file_ts > interval)
    return true;

  return false;
}

#define YMDHMS_FORMAT "%Y%m%d%H%M%S"
#define YMDHMS_SIZE 14
#define TIMESTAMP_SIZE 10

bool NormalPathFormat::BuildHdfsPath(
    const std::string& name, std::string* path) const {
  if (name.empty() || !path) {
    LOG(WARNING) << "NormalPathFormat BuildHdfsPath invalid parameters";
    return false;
  }

  std::vector<std::string> vec = SplitString(name, ".",
          kTrimWhitespace, kSplitAll);
  if (vec.size() < 4) {
    LOG(WARNING) << "NormalPathFormat BuildHdfsPath invalid name["
                 << name << "]";
    return false;
  }

  if (vec[0] != section_) {
    LOG(WARNING) << "NormalPathFormat BuildHdfsPath invalid name["
                 << name << "]";
    return false;
  }

  std::map<char, std::string> m;
  if (!format_->ParseKey(vec[1], &m)) {
    LOG(WARNING) << "NormalPathFormat BuildHdfsPath ParseKey["
                 << name << "] failed";
    return false;
  }

  struct tm timeinfo;
  if (strptime(vec[2].c_str(), YMDHMS_FORMAT, &timeinfo) == NULL) {
    LOG(WARNING) << "NormalPathFormat BuildHdfsPath strptime["
                 << name << "] failed";
    return false;
  }

  std::string path_format = conf_->hdfs_path();
  std::ostringstream os;
  auto end = path_format.end();
  for (auto it = path_format.begin(); it != end; ++it) {
    if (*it == '%') {
      ++it;
      if (it == end) {
        LOG(WARNING) << "NormalPathFormat BuildHdfsPath Invalid path_format["
                     << path_format << "]";
        return false;
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
          os << section_;
          break;
        case 'D':
          os << m['D'];
          break;
        case 'T':
          os << m['T'];
          break;
        case 't':
          os << vec[3];
          break;
        default:
          LOG(WARNING) << "NormalPathFormat BuildHdfsPath unknown formt["
                       << path_format << "]";
          return false;
      }
    } else {
      os << *it;
    }
  }
  path->assign(os.str());
  return true;
}

}   // namespace log2hdfs
