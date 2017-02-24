// Copyright (c) 2017 Lanceolata

#include "kafka2hdfs/hdfs_handle.h"

namespace log2hdfs {

std::shared_ptr<HdfsHandle> HdfsHandle::Init(const std::string &namenode,
                                             uint16_t port,
                                             const std::string &user) {
  if (namenode.empty() || user.empty()) {
    return nullptr;
  }

  hdfsFS fs_handle = hdfsConnectAsUser(namenode.c_str(), port, user.c_str());
  if (fs_handle == NULL) {
    return nullptr;
  }

  return std::shared_ptr<HdfsHandle>(new HdfsHandle(fs_handle));
}

bool HdfsHandle::FileExists(const std::string &path) const {
  if (path.empty()) {
    return false;
  }
  return FileExists(path.c_str());
}

bool HdfsHandle::FileExists(const char *path) const {
  if (!path) {
    return false;
  }
  return hdfsExists(fs_handle_, path) == 0;
}

bool HdfsHandle::CreateDirectory(const std::string &path) const {
  if (path.empty()) {
    return false;
  }
  return CreateDirectory(path.c_str());
}

bool HdfsHandle::CreateDirectory(const char *path) const {
  if (!path) {
    return false;
  }
  return hdfsCreateDirectory(fs_handle_, path) == 0;
}

}   // namespace log2hdfs
