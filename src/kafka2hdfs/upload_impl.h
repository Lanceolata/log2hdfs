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

class UploadImpl : public Upload {
 public:
  UploadImpl(std::shared_ptr<TopicConf> conf,
             std::shared_ptr<PathFormat> format,
             std::shared_ptr<FpCache> fp_cache,
             std::shared_ptr<HdfsHandle> handle):
      conf_(std::move(conf)), format_(std::move(format)),
      fp_cache_(std::move(fp_cache)), handle_(std::move(handle)) {}

  ~UploadImpl() {
    Stop();
  }

  void Start() {
    std::lock_guard<std::mutex> guard(mutex_);
    if (!thread_.joinable()) {
      running_.store(true);
      std::thread t(&UploadImpl::StartInternal, this);
      thread_ = std::move(t);
    }
  }

  virtual void StartInternal();

  void Stop() {
    running_.store(false);
    std::lock_guard<std::mutex> guard(mutex_);
    if (thread_.joinable())
      thread_.join();
  }

  virtual void Remedy() {}

  virtual void Compress() = 0;

  virtual void Upload() = 0;

 protected:
  std::shared_ptr<TopicConf> conf_;
  std::shared_ptr<PathFormat> format_;
  std::shared_ptr<FpCache> fp_cache_;
  std::shared_ptr<HdfsHandle> handle_;
  mutable std::mutex mutex_;
  std::thread thread_;
  std::atomic<int> running_;
  Queue<std::string> compress_queue_;
  Queue<std::string> upload_queue_;
};

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
      pool_(conf_->parallel()) {}

  void Compress();

  void Upload();

  void UploadPath(const std::string& path);

 protected:
  ThreadPool pool_;
};
/*
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

  void Compress(const std::string& path);

  void Upload(const std::string& path);

 protected:
  ThreadPool pool_;
};

class OrcUploadImpl : public UploadImpl {

};

class CompressUploadImpl : public UploadImpl {

};
*/
}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_UPLOAD_IMPL_H_
