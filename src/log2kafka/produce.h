// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_LOG2KAFKA_PRODUCE_H_
#define LOG2HDFS_LOG2KAFKA_PRODUCE_H_

#include <memory>
#include <mutex>
#include <atomic>
#include <thread>
#include <unordered_map>
#include "util/queue.h"

namespace log2hdfs {

class KafkaProducer;
class KafkaTopicProducer;
class TopicConf;
class OffsetTable;
class ErrmsgHandle;

class Produce {
 public:
  static std::unique_ptr<Produce> Init(
      std::shared_ptr<KafkaProducer> producer,
      std::shared_ptr<Queue<std::string>> queue,
      std::shared_ptr<OffsetTable> table,
      std::shared_ptr<ErrmsgHandle> handle);

  Produce(std::shared_ptr<KafkaProducer> producer,
          std::shared_ptr<Queue<std::string>> queue,
          std::shared_ptr<OffsetTable> table,
          std::shared_ptr<ErrmsgHandle> handle):
      producer_(std::move(producer)), queue_(std::move(queue)),
      table_(std::move(table)), handle_(std::move(handle)) {}

  ~Produce() {
    Stop();
  }

  Produce(const Produce& other) = delete;
  Produce& operator=(const Produce& other) = delete;

  bool AddTopic(std::shared_ptr<TopicConf> conf);

  bool RemoveTopic(const std::string& topic);

  void Start() {
    std::lock_guard<std::mutex> guard(thread_mutex_);
    if (!thread_.joinable()) {
      running_.store(true);
      std::thread t(&Produce::StartInternal, this);
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

  void ProduceAndSave(
      const std::string& topic,
      const std::string& path,
      off_t offset,
      int batch,
      int timeout,
      int msgs_num,
      std::shared_ptr<KafkaTopicProducer> ktp);

  std::shared_ptr<KafkaProducer> producer_;
  std::shared_ptr<Queue<std::string>> queue_;
  std::shared_ptr<OffsetTable> table_;
  std::shared_ptr<ErrmsgHandle> handle_;
  std::atomic<bool> running_;
  mutable std::mutex mutex_;
  std::mutex thread_mutex_;
  std::thread thread_;
  std::unordered_map<std::string,
      std::shared_ptr<TopicConf>> topic_confs_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_LOG2KAFKA_PRODUCE_H_
