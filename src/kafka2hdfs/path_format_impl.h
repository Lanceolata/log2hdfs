// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_PATH_FORMAT_IMPL_H_
#define LOG2HDFS_KAFKA2HDFS_PATH_FORMAT_IMPL_H_

#include "kafka2hdfs/path_format.h"

namespace log2hdfs {

// ------------------------------------------------------------------
// NormalPathFormat

class NormalFormatImpl : public PathFormat {
 public:
  static std::shared_ptr<PathFormat> Init(
      std::shared_ptr<LogFormat> log_format,
      std::shared_ptr<Kafka2hdfsTopicConf> conf);

  NormalFormatImpl(std::shared_ptr<LogFormat> log_format,
                   std::shared_ptr<Kafka2hdfsTopicConf> conf):
      log_format_(std::move(log_format)), conf_(std::move(conf)) {}

  ~NormalFormatImpl() {}

  Optional<std::string> BuildLocalPathFromMsg(const KafkaMessage *msg) const;

  bool WriteFinished(const std::string &filepath) const;

  Optional<std::string> BuildHdfsPathFromLocalpath(
      const std::string &filepath) const;
  
 protected:
  std::shared_ptr<LogFormat> log_format_;
  std::shared_ptr<Kafka2hdfsTopicConf> conf_;
};

// ------------------------------------------------------------------
// DelayPathFormat

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_PATH_FORMAT_IMPL_H_
