// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA_KAKFA_PARTITION_CONSUMER_H_
#define LOG2HDFS_KAFKA_KAKFA_PARTITION_CONSUMER_H_

#include <string>
#include <memory>
#include <atomic>
#include <mutex>
#include <thread>
#include <utility>

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
class KafkaTopicHandle;
class KafkaMessage;

class KafkaPartitionConsumeCb {
 public:
  virtual void Consume(const KafkaMessage &msg) = 0;

  virtual ~KafkaPartitionConsumeCb() {}
};

class KafkaPartitionConsumer {
 public:
  static std::shared_ptr<KafkaPartitionConsumer> Init(
      std::shared_ptr<KafkaHandle> handle,
      std::shared_ptr<KafkaTopicHandle> topic_handle,
      int32_t partition, int64_t offset,
      std::shared_ptr<KafkaPartitionConsumeCb> cb);

  KafkaPartitionConsumer(
      std::shared_ptr<KafkaHandle> handle,
      std::shared_ptr<KafkaTopicHandle> topic_handle,
      int32_t partition, int64_t offset,
      std::shared_ptr<KafkaPartitionConsumeCb> cb):
      handle_(handle), topic_handle_(topic_handle), partition_(partition),
      offset_(offset), cb_(cb), running_(false) {}

  ~KafkaPartitionConsumer() {
    Stop();
  }

  KafkaPartitionConsumer(const KafkaPartitionConsumer &other) = delete;
  KafkaPartitionConsumer &operator=(
      const KafkaPartitionConsumer &other) = delete;

  // only rd_kafka_consume_start failed return false
  bool Start();

  void StartInternal();

  void Stop();

  const std::string TopicName() const {
    return topic_handle_->Name();
  }

  int32_t partition() const {
    return partition_;
  }

  int64_t offset() const {
    return offset_;
  }

  void set_offset(int64_t offset) {
    offset_ = offset;
  }

  std::shared_ptr<KafkaPartitionConsumeCb> cb() {
    return cb_;
  }

  bool set_cb(std::shared_ptr<KafkaPartitionConsumeCb> cb) {
    if (!cb || !cb.get()) {
      return false;
    }
    cb_ = std::move(cb);
    return true;
  }

 private:
  std::shared_ptr<KafkaHandle> handle_;
  std::shared_ptr<KafkaTopicHandle> topic_handle_;
  int32_t partition_;
  int64_t offset_;
  std::shared_ptr<KafkaPartitionConsumeCb> cb_;
  std::atomic<bool> running_;
  std::mutex mutex_;
  std::thread thread_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAKFA_PARTITION_CONSUMER_H_
