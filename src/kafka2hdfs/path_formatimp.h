// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_PATH_FORMATIMP_H_
#define LOG2HDFS_KAFKA2HDFS_PATH_FORMATIMP_H_

#include "kafka2hdfs/path_format.h"

namespace log2hdfs {

// ------------------------------------------------------------------
// PathFormatimp

class PathFormatImp : public PathFormat {
 public:
  PathFormatImp(const std::string &topic_name, const std::string &local_dir,
                const std::string &upload_dir,
                const std::string &hdfs_path_format, int interval):
      topic_name_(topic_name), local_dir_(local_dir), upload_dir_(upload_dir),
      hdfs_path_format_(hdfs_path_format), interval_(interval) {}

  ~PathFormatImp() {}

  Optional<std::string> BuildLocalPathFromMsg(const Message *msg) const;

  Optional<time_t> ExtractTimeStampFromFilename(
      const std::string &filename) const;

  Optional<std::string> BuildHdfsPathFromLocalpath(
      const std::string &filepath) const;

  virtual bool ExtractInfoFromPayload(const char *payload, time_t *ts,
                                      std::string *device) const = 0;

 protected:
  std::string topic_name_;
  std::string local_dir_;
  std::string upload_dir_;
  std::string hdfs_path_format_;
  int interval_;
};

// ------------------------------------------------------------------
// V6PathFormat

class V6PathFormat : public PathFormatImp {
 public:
  static std::shared_ptr<PathFormat> Init(
      const std::string &topic_name, const std::string &local_dir,
      const std::string &upload_dir, const std::string &hdfs_path_format,
      int interval);

  V6PathFormat(const std::string &topic_name,
               const std::string &local_dir,
               const std::string &upload_dir,
               const std::string &hdfs_path_format,
               int interval):
      PathFormatImp(topic_name, local_dir, upload_dir,
                    hdfs_path_format, interval) {}

  V6PathFormat(const V6PathFormat &other):
      PathFormatImp(other.topic_name_, other.local_dir_, other.upload_dir_,
                    other.hdfs_path_format_, other.interval_) {}

  ~V6PathFormat() {}

  V6PathFormat &operator=(const V6PathFormat &other) {
    if (this != &other) {
      topic_name_ = other.topic_name_;
      local_dir_ = other.local_dir_;
      upload_dir_ = other.upload_dir_;
      hdfs_path_format_ = other.hdfs_path_format_;
      interval_ = other.interval_;
    }
    return *this;
  }

  bool ExtractInfoFromPayload(const char *payload, time_t *ts,
                              std::string *device) const;
};

// ------------------------------------------------------------------
// EfPathFormat

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_PATH_FORMATIMP_H_
