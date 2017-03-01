// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_HDFS_HANDLE_H_
#define LOG2HDFS_KAFKA2HDFS_HDFS_HANDLE_H_

#include <memory>
#include <string>
#include "hdfs.h"

namespace log2hdfs {

class HdfsHandle {
 public:
  static std::shared_ptr<HdfsHandle> Init(const std::string &namenode,
                                          uint16_t port,
                                          const std::string &user);

  HdfsHandle(const HdfsHandle &other) = delete;
  HdfsHandle &operator=(const HdfsHandle &other) = delete;

  ~HdfsHandle() {
    if (fs_handle_) {
      hdfsDisconnect(fs_handle_);
    }
  }

  bool FileExists(const std::string &path) const;

  bool FileExists(const char *path) const;

  bool CreateDirectory(const std::string &path) const;

  bool CreateDirectory(const char *path) const;

 private:
  explicit HdfsHandle(hdfsFS fs_handle): fs_handle_(fs_handle) {}

  hdfsFS fs_handle_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_HDFS_HANDLE_H_
