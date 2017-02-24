// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_UPLOADIMP_H_
#define LOG2HDFS_KAFKA2HDFS_UPLOADIMP_H_

#include <string>
#include <memory>
#include "kafka2hdfs/upload.h"
#include "kafka2hdfs/hdfs_handle.h"

namespace log2hdfs {

// ------------------------------------------------------------------
// UploadImp

class UploadImp : public Upload {
 public:
  explicit UploadImp(std::shared_ptr<HdfsHandle> fs_handle):
      fs_handle_(fs_handle) {}

  bool HdfsExists(const char *path) const {
    return fs_handle_->FileExists(path);
  }

  bool HdfsCreateDirectory(const std::string &path) const;

 private:
  std::shared_ptr<HdfsHandle> fs_handle_;
};

// ------------------------------------------------------------------
// TextUpload

class TextUpload : public UploadImp {
 public:
  static std::unique_ptr<Upload> Init(
      std::shared_ptr<Queue<std::string> > upload_queue,
      std::shared_ptr<PathFormat> path_format,
      std::shared_ptr<HdfsHandle> fs_handle,
      const std::string &upload_dir);

  TextUpload(const TextUpload &other) = delete;
  TextUpload &operator=(const TextUpload &other) = delete;

  ~TextUpload() {}

  void Start();

  void Stop();

 private:
  TextUpload(std::shared_ptr<Queue<std::string> > upload_queue,
             std::shared_ptr<PathFormat> path_format,
             std::shared_ptr<HdfsHandle> fs_handle,
             const std::string &upload_dir):
      UploadImp(fs_handle), upload_queue_(upload_queue),
      path_format_(path_format), upload_dir_(upload_dir) {}

  std::shared_ptr<Queue<std::string> > upload_queue_;
  std::shared_ptr<PathFormat> path_format_;
  std::string upload_dir_;
  bool running_ = false;
};

// ------------------------------------------------------------------
// LzoUpload

class LzoUpload : public UploadImp {
 public:
  static std::unique_ptr<Upload> Init(
      std::shared_ptr<Queue<std::string> > upload_queue,
      std::shared_ptr<PathFormat> path_format,
      std::shared_ptr<HdfsHandle> fs_handle,
      const std::string &upload_dir,
      const std::string &index_cmd);

  void Start();

  void Stop();

 private:
  LzoUpload(std::shared_ptr<Queue<std::string> > upload_queue,
            std::shared_ptr<PathFormat> path_format,
            std::shared_ptr<HdfsHandle> fs_handle,
            const std::string &upload_dir,
            const std::string &index_cmd):
      UploadImp(fs_handle), upload_queue_(upload_queue),
      path_format_(path_format), upload_dir_(upload_dir),
      index_cmd_(index_cmd) {}

  std::shared_ptr<Queue<std::string> > upload_queue_;
  std::shared_ptr<PathFormat> path_format_;
  std::string upload_dir_;
  std::string index_cmd_;
  bool running_ = false;
};

// ------------------------------------------------------------------
// OrcUpload

class OrcUpload : public UploadImp {
 public:
  static std::unique_ptr<Upload> Init(
      std::shared_ptr<Queue<std::string> > upload_queue,
      std::shared_ptr<PathFormat> path_format,
      std::shared_ptr<HdfsHandle> fs_handle,
      const std::string &upload_dir);

  void Start();

  void Stop();

 private:
  OrcUpload(std::shared_ptr<Queue<std::string> > upload_queue,
            std::shared_ptr<PathFormat> path_format,
            std::shared_ptr<HdfsHandle> fs_handle,
            const std::string &upload_dir):
      UploadImp(fs_handle), upload_queue_(upload_queue),
      path_format_(path_format), upload_dir_(upload_dir) {}

  std::shared_ptr<Queue<std::string> > upload_queue_;
  std::shared_ptr<PathFormat> path_format_;
  std::string upload_dir_;
  bool running_ = false;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_UPLOADIMP_H_
