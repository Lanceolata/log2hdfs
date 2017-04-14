// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA_KAFKA_TOPIC_H_
#define LOG2HDFS_KAFKA_KAFKA_TOPIC_H_

#include <string>
#include <memory>
#include <mutex>
#include <unordered_map>

#ifdef __cplusplus
extern "C" {
#endif
#include "librdkafka/rdkafka.h"
#ifdef __cplusplus
}
#endif

#include "kafka/kafka_topic_handle.h"

namespace log2hdfs {

class KafkaHandle;
class KafkaPartitionConsumer;
class KafkaPartitionConsumeCb;

class KafkaTopic {
 public:
  static std::shared_ptr<KafkaTopic> Init(
      rd_kafka_topic_t *rkt, std::shared_ptr<KafkaHandle> handle);

  KafkaTopic(rd_kafka_topic_t *rkt, std::shared_ptr<KafkaHandle> handle):
      handle_(handle), topic_handle_(KafkaTopicHandle::Init(rkt)) {}

  ~KafkaTopic() {}

  KafkaTopic(const KafkaTopic &other) = delete;
  KafkaTopic &operator=(const KafkaTopic &other) = delete;

  const std::string Name() const {
    return topic_handle_->Name();
  }

  std::shared_ptr<KafkaPartitionConsumer> CreatePartitionConsumer(
      int32_t partition, int64_t offset,
      std::shared_ptr<KafkaPartitionConsumeCb> cb);

  // only no KafkaPartitionConsumers return false;
  bool StartAllPartitionConsumer();

  bool StopAllPartitionConsumer();

  // TODO(lanceolata)
  // bool StartPartitionConsumers(int32_t partition);
  // bool StopPartitionConsumers(int32_t partition);

 private:
  std::mutex mutex_;
  std::shared_ptr<KafkaHandle> handle_;
  std::shared_ptr<KafkaTopicHandle> topic_handle_;
  std::unordered_map<int32_t,
      std::shared_ptr<KafkaPartitionConsumer>> partition_consumers_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAFKA_TOPIC_H_
