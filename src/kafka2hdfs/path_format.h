// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_PATH_FORMAT_H_
#define LOG2HDFS_KAFKA2HDFS_PATH_FORMAT_H_

#include <string>
#include <memory>
#include "util/optional.h"

namespace log2hdfs {

class Message;

class PathFormat {
 public:
  enum Type {
    kV6Log,
    kEfLog
  };

  static std::shared_ptr<PathFormat> Init(
      PathFormat::Type type, const std::string &topic_name,
      const std::string &local_dir, const std::string &upload_dir,
      const std::string &hdfs_path_format, int interval);

  virtual ~PathFormat() {}

  virtual Optional<std::string> BuildLocalPathFromMsg(
      const Message *msg) const = 0;

  virtual Optional<time_t> ExtractTimeStampFromFilename(
      const std::string &filename) const = 0;

  virtual Optional<std::string> BuildHdfsPathFromLocalpath(
      const std::string &filepath) const = 0;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_PATH_FORMAT_H_
