// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA_KAFKA_TOPIC_CONSUMER_H_
#define LOG2HDFS_KAFKA_KAFKA_TOPIC_CONSUMER_H_

#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <atomic>
#include <thread>
#include <unordered_map>

#ifdef __cplusplus
extern "C" {
#endif
#include "librdkafka/rdkafka.h"
#ifdef __cplusplus
}
#endif

#include "kafka/kafka_handle.h"
#include "kafka/kafka_topic.h"
#include "kafka/kafka_message.h"

namespace log2hdfs {

/**
 * Consume callback class.
 */
class KafkaConsumeCb {
 public:
  /**
   * The consume callback is used with log2hdfs::KafkaTopicConsumer
   * adn will be called for each consumed message.
   */
  virtual void Consume(const KafkaMessage& msg) = 0;

  virtual ~KafkaConsumeCb() {}
};

class KafkaTopicConsumer {
 public:
  static std::shared_ptr<KafkaTopicConsumer> Init(
      std::shared_ptr<KafkaHandle> handle,
      std::shared_ptr<KafkaTopic> topic,
      const std::vector<int32_t>& partitions,
      const std::vector<int64_t>& offsets,
      std::shared_ptr<KafkaConsumeCb> cb);

  KafkaTopicConsumer(
      std::shared_ptr<KafkaHandle> handle,
      std::shared_ptr<KafkaTopic> topic,
      const std::vector<int32_t>& partitions,
      const std::vector<int64_t>& offsets,
      std::shared_ptr<KafkaConsumeCb> cb):
      handle_(std::move(handle)), topic_(std::move(topic)),
      partitions_(partitions), offsets_(offsets),
      cb_(std::move(cb)) {}

  ~KafkaTopicConsumer() {
    Stop();
    Join();
  }

  KafkaTopicConsumer(const KafkaTopicConsumer& other) = delete;
  KafkaTopicConsumer &operator=(const KafkaTopicConsumer& other) = delete;

  const std::string Name() const {
    return topic_->Name();
  }

  void Start();

  void Stop();

  void Join();

 private:
  void StartInternal(int32_t partition);

  std::shared_ptr<KafkaHandle> handle_;
  std::shared_ptr<KafkaTopic> topic_;
  std::vector<int32_t> partitions_;
  std::vector<int64_t> offsets_;
  std::shared_ptr<KafkaConsumeCb> cb_;
  std::mutex mutex_;
  std::atomic<bool> running_;
  std::vector<std::thread> threads_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAFKA_TOPIC_CONSUMER_H_
