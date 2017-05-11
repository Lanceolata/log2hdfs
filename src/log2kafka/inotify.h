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

class Inotify {
 public:
  static std::unique_ptr<Inotify> Init(
      std::shared_ptr<Queue<std::string>> queue,
      std::shared_ptr<OffsetTable> table);

  Inotify(int inot_fd,
          std::shared_ptr<Queue<std::string>> queue,
          std::shared_ptr<OffsetTable> table):
      inot_fd_(inot_fd), queue_(std::move(queue)), table_(std::move(table)) {}

  bool AddWatchTopic(std::shared_ptr<TopicConf> conf);

  bool RemoveWatchTopic(const std::string& topic);

  bool AddWatchPath(const std::string& topic, const std::string& path,
                    time_t remedy);

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
