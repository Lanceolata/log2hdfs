// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_LOG_FORMAT_H_
#define LOG2HDFS_KAFKA2HDFS_LOG_FORMAT_H_

#include <string>
#include <memory>
#include "util/optional.h"

namespace log2hdfs {

class Kafka2hdfsTopicConf;

class LogFormat {
 public:
  enum Type {
    kV6Log,
    kEfLog
  };

  static Optional<LogFormat::Type> GetTypeFromString(const std::string &type);

  static std::shared_ptr<LogFormat> Init(LogFormat::Type type);

  virtual ~LogFormat() {}

  virtual bool ExtractInfoFromPayload(
      const char *payload,
      time_t *ts,
      std::string *device,
      std::string *type) const = 0;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_LOG_FORMAT_H_
