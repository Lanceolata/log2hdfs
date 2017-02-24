// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_UPLOAD_H_
#define LOG2HDFS_KAFKA2HDFS_UPLOAD_H_

#include <string>
#include <memory>
#include "util/queue.h"

namespace log2hdfs {

class PathFormat;
class HdfsHandle;

class Upload {
 public:
  enum Type {
    kText,
    kLzo,
    kOrc
  };

  static std::unique_ptr<Upload> Init(
      Upload::Type type,
      std::shared_ptr<Queue<std::string> > upload_queue,
      std::shared_ptr<PathFormat> path_format,
      std::shared_ptr<HdfsHandle> fs_handle,
      const std::string &upload_dir,
      const std::string &index_cmd);

  virtual ~Upload() {}

  virtual void Start() = 0;

  virtual void Stop() = 0;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_UPLOAD_H_
