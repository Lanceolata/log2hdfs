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
 * Consume callback object
 */
class KafkaConsumeCb {
 public:
  /**
   * The consume callback is used with log2hdfs::KafkaTopicConsumer
   * and will be called for each consumed message.
   */
  virtual void Consume(const KafkaMessage& msg) = 0;

  virtual ~KafkaConsumeCb() {}
};

/**
 * Kafka topic consumer
 */
class KafkaTopicConsumer {
 public:
  /**
   * Static function to create a  KafkaTopicConsumer shared_ptr.
   *
   * @param handle             KafkaHandle shared_ptr
   * @param topic              KafkaTopic shared_ptr
   * @param partitions         topic partitions
   * @param offsets            partitions offsets
   * @param cb                 topic consume callback
   *  
   * @returns std::shared_ptr<KafkaTopicConsumer>
   */
  static std::shared_ptr<KafkaTopicConsumer> Init(
      std::shared_ptr<KafkaHandle> handle,
      std::shared_ptr<KafkaTopic> topic,
      const std::vector<int32_t>& partitions,
      const std::vector<int64_t>& offsets,
      std::shared_ptr<KafkaConsumeCb> cb);

  /**
   * Constructor
   *  
   * @param handle             KafkaHandle shared_ptr
   * @param topic              KafkaTopic shared_ptr
   * @param partitions         topic partitions
   * @param offsets            partitions offsets
   * @param cb                 topic consume callback
   */
  KafkaTopicConsumer(
      std::shared_ptr<KafkaHandle> handle,
      std::shared_ptr<KafkaTopic> topic,
      const std::vector<int32_t>& partitions,
      const std::vector<int64_t>& offsets,
      std::shared_ptr<KafkaConsumeCb> cb):
      handle_(std::move(handle)), topic_(std::move(topic)),
      partitions_(partitions), offsets_(offsets),
      cb_(std::move(cb)), stop_(true) {}

  /**
   * Destructor
   * 
   * Stop all consume threads and join them.
   */
  ~KafkaTopicConsumer() {
    Stop();
    Join();
  }

  KafkaTopicConsumer(const KafkaTopicConsumer& other) = delete;
  KafkaTopicConsumer &operator=(const KafkaTopicConsumer& other) = delete;

  /**
   * @returns Topic name
   */
  const std::string Name() const {
    return topic_->Name();
  }

  /**
   * Start all consume threads.
   */
  void Start();

  /**
   * Stop all consume threads.
   */
  void Stop();

  /**
   * Join all consume threads.
   * 
   * Need to call Stop() before call Join().
   */
  void Join();

 private:
  void StartInternal(int32_t partition);

  std::shared_ptr<KafkaHandle> handle_;
  std::shared_ptr<KafkaTopic> topic_;
  std::vector<int32_t> partitions_;
  std::vector<int64_t> offsets_;
  std::shared_ptr<KafkaConsumeCb> cb_;
  mutable std::mutex mutex_;
  std::atomic<bool> stop_;
  std::vector<std::thread> threads_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAFKA_TOPIC_CONSUMER_H_
