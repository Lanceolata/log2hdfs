// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA_KAFKA_TOPIC_H_
#define LOG2HDFS_KAFKA_KAFKA_TOPIC_H_

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

class Producer;
class Consumer;
class TopicPartitionConsumer;

class Topic {
 public:
  static std::shared_ptr<Topic> Init(rd_kafka_topic_t *rkt);

  Topic(const Topic &t) = delete;
  Topic &operator=(const Topic &t) = delete;

  ~Topic() {
    if (rkt_) {
      rd_kafka_topic_destroy(rkt_);
    }
  }

  const std::string Name() const {
    return std::string(rd_kafka_topic_name(rkt_));
  }

 private:
  friend class Producer;
  friend class Consumer;
  friend class TopicPartitionConsumer;

  explicit Topic(rd_kafka_topic_t *rkt): rkt_(rkt) {}

  rd_kafka_topic_t *rkt_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAFKA_TOPIC_H_
