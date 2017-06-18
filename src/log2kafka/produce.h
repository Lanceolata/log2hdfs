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

/**
 * Produce thread.
 * 
 * Produce messages to kafka.
 */
class Produce {
 public:
  /**
   * Static function to create Produce unique_ptr.
   * 
   * @param producer            kafka producer
   * @param queue               file path queue
   * @param table               offset table
   * @param handle              error message handler
   * 
   * @returns std::unique_ptr<Produce> if init success,
   *          nullptr otherwise.
   */
  static std::unique_ptr<Produce> Init(
      std::shared_ptr<KafkaProducer> producer,
      std::shared_ptr<Queue<std::string>> queue,
      std::shared_ptr<OffsetTable> table,
      std::shared_ptr<ErrmsgHandle> handle);

  /**
   * Constructor
   */
  Produce(std::shared_ptr<KafkaProducer> producer,
          std::shared_ptr<Queue<std::string>> queue,
          std::shared_ptr<OffsetTable> table,
          std::shared_ptr<ErrmsgHandle> handle):
      producer_(std::move(producer)), queue_(std::move(queue)),
      table_(std::move(table)), handle_(std::move(handle)),
      stop_(true) {}

  ~Produce() {
    Stop();
  }

  Produce(const Produce& other) = delete;
  Produce& operator=(const Produce& other) = delete;

  /**
   * Add topic to produce
   * 
   * @param conf                topic conf
   * 
   * @return True if add success, fail otherwise.
   */
  bool AddTopic(std::shared_ptr<TopicConf> conf);

  /**
   * Remove topic from produce
   * 
   * @param topic               topic name
   * 
   * @returns True if remove success, fail otherwise.
   */
  bool RemoveTopic(const std::string& topic);

  /**
   * Start produce thread.
   * 
   * Pop file from queue and produce messages to kafka.
   */
  void Start() {
    std::lock_guard<std::mutex> lock(thread_mutex_);
    if (!thread_.joinable()) {
      stop_.store(false);
      std::thread t(&Produce::StartInternal, this);
      thread_ = std::move(t);
    }
  }

  /**
   * Stop produce thread.
   */
  void Stop() {
    stop_.store(true);
    std::lock_guard<std::mutex> lock(thread_mutex_);
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
  std::atomic<bool> stop_;
  mutable std::mutex mutex_;
  std::mutex thread_mutex_;
  std::thread thread_;
  std::unordered_map<std::string,
      std::shared_ptr<TopicConf>> topic_confs_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_LOG2KAFKA_PRODUCE_H_
