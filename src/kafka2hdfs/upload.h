// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_UPLOAD_H_
#define LOG2HDFS_KAFKA2HDFS_UPLOAD_H_

#include <string>
#include <memory>
#include "util/queue.h"
#include "util/optional.h"

namespace log2hdfs {

class HdfsHandle;
class PathFormat;
class TopicConf;

class Upload {
 public:
  enum Type {
    kText,
    kLzo,
    kOrc,
    kCompress,
    kTextNoUpload
  };

  static Optional<Upload::Type> ParseType(const std::string& type);

  static std::unique_ptr<Upload> Init(std::shared_ptr<TopicConf> conf);

  virtual ~Upload() {}

  virtual bool Start() = 0;

  virtual void Stop() = 0;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_COMPRESS_UPLOAD_H_
