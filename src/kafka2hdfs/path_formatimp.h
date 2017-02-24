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

 protected:
  virtual bool ExtractInfoFromPayload(char *payload, time_t *ts,
                                      std::string *device) const = 0;
  std::string topic_name_;
  std::string local_dir_;
  std::string upload_dir_;
  std::string hdfs_path_format_;
  int interval_;
};

// ------------------------------------------------------------------
// V6PathFormat

class V6PathFormat : public PathFormat {
 public:
  static std::shared_ptr<PathFormat> Init(
      const std::string &topic_name, const std::string &local_dir,
      const std::string &hdfs_path_format, int interval);
  
  V6PathFormat(const std::string &topic_name, const std::string &local_dir,
               const std::string &hdfs_path_format, int interval):
      topic_name_(topic_name), local_dir_(local_dir),
      hdfs_path_format_(hdfs_path_format) {}

  V6PathFormat(const V6PathFormat &other) {
    topic_name_ = other.topic_name_;
    local_dir_ = other.local_dir_;
    hdfs_path_format_ = other.hdfs_path_format_;
  }

  V6PathFormat &operator=(const V6PathFormat &other) {
    if (this != &other) {
      topic_name_ = other.topic_name_;
      local_dir_ = other.local_dir_;
      hdfs_path_format_ = other.hdfs_path_format_;
    }
    return *this;
  }

  ~V6PathFormat() {}

  Optional<std::string> BuildLocalPathFromMsg(const Message *msg) const;

  Optional<time_t> ExtractTimeStampFromFilename(
      const std::string &filename) const;

  Optional<std::string> BuildHdfsPathFromLocalpath(
      const std::string &filepath) const;

 private:
  struct FormatInfo {
    time_t time_stamp;
    std::string device_type;
  };

  std::unique_ptr<FormatInfo> ExtractFromPayload(const char *payload) const;

  std::string topic_name_;
  std::string local_dir_;
  std::string hdfs_path_format_;
};

// ------------------------------------------------------------------
// EfPathFormat
/*
class EfPathFormat : public PathFormat {
 public:
  static std::shared_ptr<PathFormat> Init(
      const std::string &topic_name, const std::string &local_dir,
      const std::string &hdfs_path_format);
  
  EfPathFormat(const std::string &topic_name, const std::string &local_dir,
               const std::string &hdfs_path_format):
      topic_name_(topic_name), local_dir_(local_dir),
      hdfs_path_format_(hdfs_path_format) {}

  EfPathFormat(const EfPathFormat &other) {
    topic_name_ = other.topic_name_;
    local_dir_ = other.local_dir_;
    hdfs_path_format_ = other.hdfs_path_format_;
  }

  EfPathFormat &operator=(const EfPathFormat &other) {
    if (this != &other) {
      topic_name_ = other.topic_name_;
      local_dir_ = other.local_dir_;
      hdfs_path_format_ = other.hdfs_path_format_;
    }
    return *this;
  }

  ~EfPathFormat() {}

  Optional<std::string> BuildLocalPathFromMsg(const Message *msg) const;

  Optional<time_t> ExtractTimeStampFromFilename(
      const std::string &filename) const;

  Optional<std::string> BuildHdfsPathFromLocalpath(
      const std::string &filepath) const;

 private:
  struct FormatInfo {
    time_t time_stamp;
    std::string device_type;
  };

  FormatInfo ExtractFromPayload(char *payload) const;

  std::string topic_name_;
  std::string local_dir_;
  std::string hdfs_path_format_;
};
*/
}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_PATH_FORMATIMP_H_
