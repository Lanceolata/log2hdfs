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

class Section;

/**
 * Archive offset
 */
class OffsetTable {
 public:
  /**
   * Static function to create a OffsetTable shared_ptr
   * 
   * @param section             Ini configuration section
   * 
   * @returns std::shared_ptr<OffsetTable> if init success,
   *          nullptr otherwise.
   */
  static std::shared_ptr<OffsetTable> Init(
      std::shared_ptr<Section> section);

  /**
   * Constructor
   * 
   * @param path                archive file path
   * @param interval            archive interval
   */
  OffsetTable(const std::string& path, int interval):
      path_(path), interval_(interval), stop_(true) {
    if (IsFile(path_))
      Remedy();
  }

  /**
   * Destructor
   * 
   * stop thread.
   */
  ~OffsetTable() {
    Stop();
  }

  OffsetTable(const OffsetTable& other) = delete;
  OffsetTable& operator=(const OffsetTable& other) = delete;

  /**
   * Update offset to OffsetTable
   * 
   * @param dir                 dir path
   * @param file                file name
   * @param offset              file offset
   * 
   * @returns True if update success, false otherwise.
   */
  bool Update(const std::string& dir, const std::string& file, off_t offset);

  /**
   * Get file name and offset from offset table. 
   * 
   * @param dir                 dir to match
   * @param file                file name to set
   * @param offset              offset to set
   * 
   * @returns True if get success, false otherwise.
   */
  bool Get(const std::string& dir, std::string* file, off_t* offset) const;

  /**
   * Remove offset table record.
   * 
   * @param dir                 dir to match
   */
  bool Remove(const std::string& dir);

  /**
   * Archive to local file.
   */
  bool Save() const;

  /**
   * Load offset table from local file.
   */
  void Remedy();

  /**
   * Start archive thread.
   */
  void Start() {
    std::lock_guard<std::mutex> guard(thread_mutex_);
    if (!thread_.joinable()) {
      stop_.store(false);
      std::thread t(&OffsetTable::StartInternal, this);
      thread_ = std::move(t);
    }
  }

  /**
   * Stop archive thread.
   */
  void Stop() {
    stop_.store(true);
    std::lock_guard<std::mutex> guard(thread_mutex_);
    if (thread_.joinable())
      thread_.join();
    Save();
  }

 private:
  void StartInternal();

  class FileOffset {
   public:
    FileOffset(): filename_(), offset_(0) {}

    FileOffset(const std::string& filename, off_t offset):
        filename_(filename), offset_(offset) {}

    FileOffset(const FileOffset& other):
        filename_(other.filename_), offset_(other.offset_) {}

    FileOffset(FileOffset&& other):
        filename_(std::move(other.filename_)), offset_(other.offset_) {}

    FileOffset& operator=(const FileOffset& other) {
      if (this != &other) {
        filename_ = other.filename_;
        offset_ = other.offset_;
      }
      return *this;
    }

    FileOffset& operator=(FileOffset&& other) {
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
  std::atomic<bool> stop_;
  std::mutex thread_mutex_;
  std::thread thread_;
  std::unordered_map<std::string, FileOffset> table_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_LOG2KAFKA_OFFSET_TABLE_H_
