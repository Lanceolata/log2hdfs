// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_LOG2KAFKA_LOG2KAFKA_INOTIFY_H_
#define LOG2HDFS_LOG2KAFKA_LOG2KAFKA_INOTIFY_H_

#include <string>
#include <mutex>
#include <memory>
#include <thread>
#include <unordered_map>
#include "util/queue.h"

namespace log2hdfs {

class Log2kafkaTopicConf;
class Log2kafkaOffsetTable;

class Log2kafkaInotify {
 public:
  static std::unique_ptr<Log2kafkaInotify> Init(
      std::shared_ptr<Queue<std::string>> queue,
      std::shared_ptr<Log2kafkaOffsetTable> table);

  Log2kafkaInotify(int inot_fd, std::shared_ptr<Queue<std::string>> queue,
                   std::shared_ptr<Log2kafkaOffsetTable> table):
      inot_fd_(inot_fd), queue_(std::move(queue)), table_(std::move(table)) {}

  bool AddWatchTopic(std::shared_ptr<Log2kafkaTopicConf> conf);

  bool RemoveWatchTopic(const std::string& topic);

  bool AddWatchPath(const std::string& topic, const std::string& path,
                    time_t remedy);

  // bool RemoveWatchPath(const std::string& path);

  void Start() {
    std::call_once(flag_, &Log2kafkaInotify::CreateThread, this);
  }

 private:
  void CreateThread() {
    std::thread t(&Log2kafkaInotify::StartInternal, this);
    t.detach();
  }

  void StartInternal();

  void Remedy(const std::string& topic, const std::string& path,
              time_t remedy, const struct timespec& ts);

  int ReadInotify();

  int inot_fd_;
  std::shared_ptr<Queue<std::string>> queue_;
  std::shared_ptr<Log2kafkaOffsetTable> table_;
  mutable std::mutex mutex_;
  std::unordered_map<int, std::string> wd_topic_;
  std::unordered_map<int, std::string> wd_path_;
  std::once_flag flag_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_LOG2KAFKA_LOG2KAFKA_INOTIFY_H_
