// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA_KAFKA_TOPIC_HANDLE_H_
#define LOG2HDFS_KAFKA_KAFKA_TOPIC_HANDLE_H_

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

class KafkaPartitionConsumer;

class KafkaTopicHandle {
 public:
  static std::shared_ptr<KafkaTopicHandle> Init(rd_kafka_topic_t *rkt);

  explicit KafkaTopicHandle(rd_kafka_topic_t *rkt): rkt_(rkt) {}

  ~KafkaTopicHandle() {
    if (rkt_) {
      rd_kafka_topic_destroy(rkt_);
      rkt_ = NULL;
    }
  }

  KafkaTopicHandle(const KafkaTopicHandle &other) = delete;
  KafkaTopicHandle &operator=(const KafkaTopicHandle &other) = delete;

  const std::string Name() const {
    return std::string(rd_kafka_topic_name(rkt_));
  }

 private:
  friend class KafkaPartitionConsumer;

  rd_kafka_topic_t *rkt_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAFKA_TOPIC_HANDLE_H_
