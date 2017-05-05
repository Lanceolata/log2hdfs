// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_LOG2KAFKA_LOG2KAFKA_PRODUCE_H_
#define LOG2HDFS_LOG2KAFKA_LOG2KAFKA_PRODUCE_H_

#include <memory>
#include <mutex>
#include <atomic>
#include <thread>
#include <unordered_map>
#include "util/queue.h"

namespace log2hdfs {

class KafkaTopicProducer;
class Log2kafkaTopicConf;
class KafkaProducer;
class Log2kafkaOffsetTable;
class Log2kafkaErrmsgHandle;

class Log2kafkaProduce {
 public:
  static std::unique_ptr<Log2kafkaProduce> Init(
      std::shared_ptr<KafkaProducer> producer,
      std::shared_ptr<Queue<std::string>> queue,
      std::shared_ptr<Log2kafkaOffsetTable> table,
      std::shared_ptr<Log2kafkaErrmsgHandle> handle);

  Log2kafkaProduce(std::shared_ptr<KafkaProducer> producer,
                   std::shared_ptr<Queue<std::string>> queue,
                   std::shared_ptr<Log2kafkaOffsetTable> table,
                   std::shared_ptr<Log2kafkaErrmsgHandle> handle):
      producer_(std::move(producer)), queue_(std::move(queue)),
      table_(std::move(table)), handle_(std::move(handle)) {}

  ~Log2kafkaProduce() {
    Stop();
  }

  Log2kafkaProduce(const Log2kafkaProduce& other) = delete;
  Log2kafkaProduce& operator=(const Log2kafkaProduce& other) = delete;

  bool AddTopic(std::shared_ptr<Log2kafkaTopicConf> conf);

  bool RemoveTopic(const std::string& topic);

  void Start();

  void Stop();

 private:
  void StartInternal();

  void ProduceAndSave(const std::string& topic,
                      const std::string& path, off_t offset,
                      int batch, int timeout, int msgs_num,
                      std::shared_ptr<KafkaTopicProducer> ktp);

  std::shared_ptr<KafkaProducer> producer_;
  std::shared_ptr<Queue<std::string>> queue_;
  std::shared_ptr<Log2kafkaOffsetTable> table_;
  std::shared_ptr<Log2kafkaErrmsgHandle> handle_;
  std::atomic<bool> running_;
  mutable std::mutex mutex_;
  std::mutex thread_mutex_;
  std::thread thread_;
  std::unordered_map<std::string,
      std::shared_ptr<Log2kafkaTopicConf>> topic_confs_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_LOG2KAFKA_LOG2KAFKA_PRODUCE_H_
