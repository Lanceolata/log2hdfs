// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA_KAFKA_PARTITION_CONSUMER_H_
#define LOG2HDFS_KAFKA_KAFKA_PARTITION_CONSUMER_H_

#include <string>
#include <memory>

#ifdef __cplusplus
extern "C" {
#endif
#include "librdkafka/rdkafka.h"
#ifdef __cplusplus
}
#endif

namespace log2hdfs {

class KafkaHandle;
class KafkaTopic;
class KafkaMessage;

class KafkaPartitionConsumerCb {
 public:
  virtual void Consume(KafkaMessage *msg) = 0;

  virtual ~KafkaPartitionConsumerCb() {}
};

class KafkaPartitionConsumer {
 public:
  static std::shared_ptr<KafkaPartitionConsumer> Init(
      std::shared_ptr<KafkaHandle> handle,
      std::shared_ptr<KafkaTopic> topic,
      int32_t partition, int64_t offset,
      std::shared_ptr<KafkaPartitionConsumerCb> cb);

  explicit KafkaPartitionConsumer(
      std::shared_ptr<KafkaHandle> handle,
      std::shared_ptr<KafkaTopic> topic,
      int32_t partition, int64_t offset,
      std::shared_ptr<KafkaPartitionConsumerCb> cb):
      handle_(handle), topic_(topic), partition_(partition),
      offset_(offset), cb_(cb) {}

  KafkaPartitionConsumer(const KafkaPartitionConsumer &other) = delete;
  KafkaPartitionConsumer &operator=(
      const KafkaPartitionConsumer &other) = delete;

  ~KafkaPartitionConsumer() {}

  void Start();

  void Stop();

  std::shared_ptr<KafkaTopic> topic() {
    return topic_;
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

  std::shared_ptr<KafkaPartitionConsumerCb> cb() {
    return cb_;
  }

  bool set_cb(std::shared_ptr<KafkaPartitionConsumerCb> cb) {
    if (!cb) {
      return false;
    }
    cb_ = cb;
    return true;
  }

  bool running() const {
    return running_;
  }

 private:
  std::shared_ptr<KafkaHandle> handle_;
  std::shared_ptr<KafkaTopic> topic_;
  int32_t partition_;
  int64_t offset_;
  std::shared_ptr<KafkaPartitionConsumerCb> cb_;
  bool running_ = false;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAFKA_PARTITION_CONSUMER_H_
