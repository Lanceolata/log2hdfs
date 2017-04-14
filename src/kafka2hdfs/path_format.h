// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_PATH_FORMAT_H_
#define LOG2HDFS_KAFKA2HDFS_PATH_FORMAT_H_

#include <string>
#include <memory>
#include "util/optional.h"

namespace log2hdfs {

class KafkaMessage;
class LogFormat;
class Kafka2hdfsTopicConf;

class PathFormat {
 public:
  enum Type {
    kNormal,
    kDelay
  };
  
  static Optional<PathFormat::Type> GetTypeFromString(const std::string &type);

  static std::shared_ptr<PathFormat> Init(
      PathFormat::Type type,
      std::shared_ptr<LogFormat> log_format,
      std::shared_ptr<Kafka2hdfsTopicConf> conf);

  virtual ~PathFormat() {}

  virtual Optional<std::string> BuildLocalPathFromMsg(
      const KafkaMessage *msg) const = 0;

  virtual bool WriteFinished(const std::string &filepath) const = 0;

  virtual Optional<std::string> BuildHdfsPathFromLocalpath(
      const std::string &filepath) const = 0;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_PATH_FORMAT_H_
