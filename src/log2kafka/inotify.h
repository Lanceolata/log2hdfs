// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_LOG2KAFKA_INOTIFY_H_
#define LOG2HDFS_LOG2KAFKA_INOTIFY_H_

#include <string>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include "util/queue.h"

namespace log2hdfs {

class TopicConf;
class OffsetTable;

/**
 * linux inotify
 */
class Inotify {
 public:
  /**
   * Static function create a Inotify unique_ptr.
   * 
   * @param queue               produce queue
   * @param table               offset table
   * 
   * @returns std::unique_ptr<Inotify> if init success,
   *          nullptr otherwise.
   */
  static std::unique_ptr<Inotify> Init(
      std::shared_ptr<Queue<std::string>> queue,
      std::shared_ptr<OffsetTable> table);

  /**
   * Constructor
   */
  Inotify(int inot_fd,
          std::shared_ptr<Queue<std::string>> queue,
          std::shared_ptr<OffsetTable> table):
      inot_fd_(inot_fd), queue_(std::move(queue)), table_(std::move(table)) {}

  /**
   * Add inotify watch topic paths
   * 
   * @param conf                topic conf
   * 
   * @returns True if add inotify watch success,
   *          false otherwise.
   */
  bool AddWatchTopic(std::shared_ptr<TopicConf> conf);

  /**
   * Remove inotify watch topic paths
   * 
   * @param topic               topic to watch
   * 
   * @returns True if remove inotify watch success,
   *          false otherwise.
   */
  bool RemoveWatchTopic(const std::string& topic);

  /**
   * Add inotify watch path rescursive
   * 
   * @param topic               topic name
   * @param path                path to add inotify rescursive
   * @param remedy              remedy time
   * 
   * @returns True if add watch path success, false otherwise.
   */
  bool AddWatchPath(const std::string& topic,
                    const std::string& path,
                    time_t remedy);

  /**
   * Start inotify thread
   */
  void Start() {
    std::call_once(flag_, &Inotify::CreateThread, this);
  }

 private:
  void CreateThread() {
    std::thread t(&Inotify::StartInternal, this);
    t.detach();
  }

  void Remedy(const std::string& topic, const std::string& path,
              time_t remedy, const struct timespec& end,
              const std::string& remedy_file, off_t remedy_offset);

  void StartInternal();

  int ReadInotify();

  int inot_fd_;
  std::shared_ptr<Queue<std::string>> queue_;
  std::shared_ptr<OffsetTable> table_;
  mutable std::mutex mutex_;
  // wd <--> topic
  std::unordered_map<int, std::string> wd_topic_;
  // wd <--> path
  std::unordered_map<int, std::string> wd_path_;
  std::once_flag flag_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_LOG2KAFKA_INOTIFY_H_
