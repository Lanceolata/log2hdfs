// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_HDFS_HANDLE_H_
#define LOG2HDFS_KAFKA2HDFS_HDFS_HANDLE_H_

#include <memory>
#include <string>
#include "hdfs.h"
#include "util/optional.h"

namespace log2hdfs {

class Section;

class HdfsHandle {
 public:
  enum Type {
    kCommand,
    kApi
  };

  static std::shared_ptr<HdfsHandle> Init(
      std::shared_ptr<Section> section);

  virtual ~HdfsHandle() {}

  virtual bool Exists(const std::string& hdfs_path) const = 0;

  virtual bool Put(const std::string& local_path,
                   const std::string& hdfs_path) const = 0;

  virtual bool Append(const std::string& local_path,
                      const std::string& hdfs_path) const = 0;

  virtual bool Delete(const std::string& hdfs_path) const = 0;

  virtual bool CreateDirectory(const std::string& hdfs_path) const = 0;

  virtual bool LZOIndex(const std::string& hdfs_path) const = 0;
};

};

#endif  // LOG2HDFS_KAFKA2HDFS_HDFS_HANDLE_H_
