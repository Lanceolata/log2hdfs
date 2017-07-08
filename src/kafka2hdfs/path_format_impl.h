// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_PATH_FORMAT_IMPL_H_
#define LOG2HDFS_KAFKA2HDFS_PATH_FORMAT_IMPL_H_

#include "kafka2hdfs/path_format.h"

namespace log2hdfs {

class NormalPathFormat : public PathFormat {
 public:
  static std::shared_ptr<NormalPathFormat> Init(
      std::shared_ptr<TopicConf> conf);

  NormalPathFormat(const std::string& topic,
                   std::unique_ptr<LogFormat> format,
                   std::shared_ptr<TopicConf> conf):
      topic_(topic), format_(std::move(format)),
      conf_(std::move(conf)) {}

  bool BuildLocalFileName(const KafkaMessage& msg, std::string* name) const;

  bool WriteFinished(const std::string& filepath) const;

  bool BuildHdfsPath(const std::string& name,
                     std::string* path,
                     bool delay = false) const;

 protected:
  std::string topic_;
  std::unique_ptr<LogFormat> format_;
  std::shared_ptr<TopicConf> conf_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_PATH_FORMAT_IMPL_H_
