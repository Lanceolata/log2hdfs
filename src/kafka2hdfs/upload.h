// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_UPLOAD_H_
#define LOG2HDFS_KAFKA2HDFS_UPLOAD_H_

#include <string>
#include <memory>
#include "util/optional.h"

namespace log2hdfs {

class FpCache;
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

  static std::unique_ptr<Upload> Init(
      std::shared_ptr<TopicConf> conf,
      std::shared_ptr<PathFormat> format,
      std::shared_ptr<FpCache> fp_cache,
      std::shared_ptr<HdfsHandle> handle);

  virtual ~Upload() {}

  virtual void Start() = 0;

  virtual void Stop() = 0;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_COMPRESS_UPLOAD_H_
