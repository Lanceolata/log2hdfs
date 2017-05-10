// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_LOG2KAFKA_OFFSET_TABLE_H_
#define LOG2HDFS_LOG2KAFKA_OFFSET_TABLE_H_

#include <string>
#include <memory>
#include <mutex>
#include <thread>
#include <atomic>
#include <unordered_map>
#include "util/system_utils.h"

namespace log2hdfs {

class Fileoffset;
class Section;

class OffsetTable {
 public:
  static std::shared_ptr<OffsetTable> Init(
      std::shared_ptr<Section> section);

  OffsetTable(const std::string& path, int interval):
      path_(path), interval_(interval) {
    if (IsFile(path_))
      Remedy();
  }

  ~OffsetTable() {
    Stop();
  }

  OffsetTable(const OffsetTable& other) = delete;
  OffsetTable& operator=(const OffsetTable& other) = delete;

  bool Update(const std::string& dir, const std::string& file, off_t offset);

  bool Get(const std::string& dir, std::string* file, off_t* offset) const;

  bool Remove(const std::string& dir);

  bool Save() const;

  void Remedy();

  void Start() {
    std::lock_guard<std::mutex> guard(thread_mutex_);
    if (!thread_.joinable()) {
      running_.store(true);
      std::thread t(&OffsetTable::StartInternal, this);
      thread_ = std::move(t);
    }
  }

  void Stop() {
    running_.store(false);
    std::lock_guard<std::mutex> guard(thread_mutex_);
    if (thread_.joinable())
      thread_.join();
  }

 private:
  void StartInternal();

  class Fileoffset {
   public:
    Fileoffset(): filename_(), offset_(0) {}

    Fileoffset(const std::string& filename, off_t offset):
        filename_(filename), offset_(offset) {}

    Fileoffset(const Fileoffset& other):
        filename_(other.filename_), offset_(other.offset_) {}

    Fileoffset(Fileoffset&& other):
        filename_(std::move(other.filename_)), offset_(other.offset_) {}

    Fileoffset& operator=(const Fileoffset& other) {
      if (this != &other) {
        filename_ = other.filename_;
        offset_ = other.offset_;
      }
      return *this;
    }

    Fileoffset& operator=(Fileoffset&& other) {
      filename_ = std::move(other.filename_);
      offset_ = other.offset_;
      return *this;
    }

    std::string filename_;
    off_t offset_;
  };

  std::string path_;
  int interval_;
  mutable std::mutex mutex_;
  std::atomic<bool> running_;
  std::mutex thread_mutex_;
  std::thread thread_;
  std::unordered_map<std::string, Fileoffset> table_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_LOG2KAFKA_OFFSET_TABLE_H_
