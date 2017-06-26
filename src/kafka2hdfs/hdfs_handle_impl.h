// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_HDFS_HANDLE_IMPL_H_
#define LOG2HDFS_KAFKA2HDFS_HDFS_HANDLE_IMPL_H_

#include "kafka2hdfs/hdfs_handle.h"

namespace log2hdfs {

class CommandHdfsHandle : public HdfsHandle {
 public:
  static std::shared_ptr<CommandHdfsHandle> Init(
      std::shared_ptr<Section> section);

  CommandHdfsHandle(hdfsFS fs_handle,
                    const std::string& put,
                    const std::string& append,
                    const std::string& lzo_index):
      fs_handle_(fs_handle), put_(put), append_(append),
      lzo_index_(lzo_index) {}

  ~CommandHdfsHandle() {
    if (fs_handle_)
      hdfsDisconnect(fs_handle_);
  }

  CommandHdfsHandle(const CommandHdfsHandle& other) = delete;
  CommandHdfsHandle& operator=(const CommandHdfsHandle& other) = delete;

  bool Exists(const std::string& hdfs_path) const;

  bool Put(const std::string& local_path,
           const std::string& hdfs_path) const;

  bool Append(const std::string& local_path,
              const std::string& hdfs_path) const;

  bool Delete(const std::string& local_path) const;

  bool CreateDirectory(const std::string& hdfs_path) const;

  bool LZOIndex(const std::string& hdfs_path) const;

 private:
  hdfsFS fs_handle_;
  std::string put_;
  std::string append_;
  std::string lzo_index_;
};

// TODO class ApiHdfsHandle

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_HDFS_HANDLE_IMPL_H_
