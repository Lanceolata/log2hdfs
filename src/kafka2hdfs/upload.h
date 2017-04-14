// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_UPLOAD_H_
#define LOG2HDFS_KAFKA2HDFS_UPLOAD_H_

#include <string>
#include <memory>
#include "util/queue.h"

namespace log2hdfs {

class HdfsHandle;
class LogFormat;
class Kafka2hdfsTopicConf;

class Upload {
 public:
  enum Type {
    kText,
    kLzo,
    kOrc,
    kNone
  };

  static std::unique_ptr<Upload> Init(
      Upload::Type type, std::shared_ptr<Kafka2hdfsTopicConf> conf);

  virtual ~Upload() {}

  virtual bool Start() = 0;

  virtual void Stop() = 0;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_UPLOAD_H_
