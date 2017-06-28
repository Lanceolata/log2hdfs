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

/**
 * Upload interface
 */
class Upload {
 public:
  /**
   * Upload type enum
   */
  enum Type {
    kText,
    kLzo,
    kOrc,
    kCompress,
    kTextNoUpload
  };

  /**
   * Convert type string to Upload type
   */
  static Optional<Upload::Type> ParseType(const std::string& type);

  /**
   * Static function to create a Upload unique_ptr.
   */
  static std::unique_ptr<Upload> Init(
      std::shared_ptr<TopicConf> conf,
      std::shared_ptr<PathFormat> format,
      std::shared_ptr<FpCache> fp_cache,
      std::shared_ptr<HdfsHandle> handle);

  virtual ~Upload() {}

  /**
   * Upload thread create.
   */
  virtual void Start() = 0;

  /**
   * Uplaod thread stop.
   */
  virtual void Stop() = 0;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_UPLOAD_H_
