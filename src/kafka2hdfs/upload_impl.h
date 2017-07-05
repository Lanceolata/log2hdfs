// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_UPLOAD_IMPL_H_
#define LOG2HDFS_KAFKA2HDFS_UPLOAD_IMPL_H_

#include "kafka2hdfs/upload.h"
#include <thread>
#include <mutex>
#include <atomic>
#include "kafka2hdfs/topic_conf.h"
#include "util/queue.h"
#include "util/thread_pool.h"

namespace log2hdfs {

// ------------------------------------------------------------------
// UploadImpl

class UploadImpl : public Upload {
 public:
  UploadImpl(std::shared_ptr<TopicConf> conf,
             std::shared_ptr<PathFormat> format,
             std::shared_ptr<FpCache> fp_cache,
             std::shared_ptr<HdfsHandle> handle):
      conf_(std::move(conf)), format_(std::move(format)),
      fp_cache_(std::move(fp_cache)), handle_(std::move(handle)) {
    topic_ = conf_->topic();
    consume_dir_ = conf_->consume_dir();
    compress_dir_ = conf_->compress_dir();
    upload_dir_ = conf_->upload_dir();
  }

  ~UploadImpl() {
    Join();
  }

  void Start() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!thread_.joinable()) {
      stop_.store(false);
      std::thread t(&UploadImpl::StartInternal, this);
      thread_ = std::move(t);
    }
  }

  void Stop() {
    stop_.store(true);
  }

  void Join() {
    stop_.store(true);
    std::lock_guard<std::mutex> lock(mutex_);
    if (thread_.joinable())
      thread_.join();
  }

  virtual void StartInternal();

  virtual void Remedy();

  virtual void Compress() = 0;

  virtual void Upload() = 0;

  virtual void UploadFile(const std::string& path, bool append, bool index);

 protected:
  std::shared_ptr<TopicConf> conf_;
  std::shared_ptr<PathFormat> format_;
  std::shared_ptr<FpCache> fp_cache_;
  std::shared_ptr<HdfsHandle> handle_;
  std::string topic_;
  std::string consume_dir_;
  std::string compress_dir_;
  std::string upload_dir_;
  mutable std::mutex mutex_;
  std::thread thread_;
  std::atomic<bool> stop_;
  Queue<std::string> compress_queue_;
  Queue<std::string> upload_queue_;
};

// ------------------------------------------------------------------
// TextUploadImpl

class TextUploadImpl : public UploadImpl {
 public:
  static std::unique_ptr<TextUploadImpl> Init(
      std::shared_ptr<TopicConf> conf,
      std::shared_ptr<PathFormat> format,
      std::shared_ptr<FpCache> fp_cache,
      std::shared_ptr<HdfsHandle> handle);

  TextUploadImpl(std::shared_ptr<TopicConf> conf,
                 std::shared_ptr<PathFormat> format,
                 std::shared_ptr<FpCache> fp_cache,
                 std::shared_ptr<HdfsHandle> handle):
      UploadImpl(std::move(conf), std::move(format),
                 std::move(fp_cache), std::move(handle)),
      pool_(1) {}

  void Compress();

  void Upload();

 protected:
  ThreadPool pool_;
};

// ------------------------------------------------------------------
// LzoUploadImpl

class LzoUploadImpl : public UploadImpl {
 public:
  static std::unique_ptr<LzoUploadImpl> Init(
      std::shared_ptr<TopicConf> conf,
      std::shared_ptr<PathFormat> format,
      std::shared_ptr<FpCache> fp_cache,
      std::shared_ptr<HdfsHandle> handle);

  LzoUploadImpl(std::shared_ptr<TopicConf> conf,
                std::shared_ptr<PathFormat> format,
                std::shared_ptr<FpCache> fp_cache,
                std::shared_ptr<HdfsHandle> handle):
      UploadImpl(std::move(conf), std::move(format),
                 std::move(fp_cache), std::move(handle)),
      pool_(conf_->parallel()) {}

  void Compress();

  void CompressFile(const std::string& path);

  void Upload();

 protected:
  ThreadPool pool_;
};

// ------------------------------------------------------------------
// OrcUploadImpl

class OrcUploadImpl : public UploadImpl {
 public:
  static std::unique_ptr<OrcUploadImpl> Init(
      std::shared_ptr<TopicConf> conf,
      std::shared_ptr<PathFormat> format,
      std::shared_ptr<FpCache> fp_cache,
      std::shared_ptr<HdfsHandle> handle);

  OrcUploadImpl(std::shared_ptr<TopicConf> conf,
                std::shared_ptr<PathFormat> format,
                std::shared_ptr<FpCache> fp_cache,
                std::shared_ptr<HdfsHandle> handle):
      UploadImpl(std::move(conf), std::move(format),
                 std::move(fp_cache), std::move(handle)),
      pool_(conf_->parallel()) {}

  void Compress();

  void CompressFile(const std::string& path);

  void Upload();

 protected:
  ThreadPool pool_;
};

// ------------------------------------------------------------------
// CompressUploadImpl

class CompressUploadImpl : public UploadImpl {
 public:
  static std::unique_ptr<CompressUploadImpl> Init(
      std::shared_ptr<TopicConf> conf,
      std::shared_ptr<PathFormat> format,
      std::shared_ptr<FpCache> fp_cache,
      std::shared_ptr<HdfsHandle> handle);

  CompressUploadImpl(std::shared_ptr<TopicConf> conf,
                     std::shared_ptr<PathFormat> format,
                     std::shared_ptr<FpCache> fp_cache,
                     std::shared_ptr<HdfsHandle> handle):
      UploadImpl(std::move(conf), std::move(format),
                 std::move(fp_cache), std::move(handle)),
      pool_(conf_->parallel()) {}

  void Compress();

  void Upload();

 protected:
  ThreadPool pool_;
};

// ------------------------------------------------------------------
// TextNoUploadImpl

class TextNoUploadImpl : public UploadImpl {
 public:
  static std::unique_ptr<TextNoUploadImpl> Init(
      std::shared_ptr<TopicConf> conf,
      std::shared_ptr<PathFormat> format,
      std::shared_ptr<FpCache> fp_cache,
      std::shared_ptr<HdfsHandle> handle);

  TextNoUploadImpl(std::shared_ptr<TopicConf> conf,
                   std::shared_ptr<PathFormat> format,
                   std::shared_ptr<FpCache> fp_cache,
                   std::shared_ptr<HdfsHandle> handle):
      UploadImpl(std::move(conf), std::move(format),
                 std::move(fp_cache), std::move(handle)) {}

  void Compress();

  void Upload();
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_UPLOAD_IMPL_H_
