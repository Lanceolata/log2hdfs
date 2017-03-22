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

class KafkaProducer;
class KafkaConsumer;
class KafkaPartitionConsumer;

class KafkaTopic {
 public:
  static std::shared_ptr<KafkaTopic> Init(rd_kafka_topic_t *rkt);

  explicit KafkaTopic(rd_kafka_topic_t *rkt): rkt_(rkt) {}

  KafkaTopic(const KafkaTopic &other) = delete;
  KafkaTopic &operator=(const KafkaTopic &other) = delete;

  ~KafkaTopic() {
    if (rkt_) {
      rd_kafka_topic_destroy(rkt_);
    }
  }

  const std::string Name() const {
    return std::string(rd_kafka_topic_name(rkt_));
  }

 private:
  friend class KafkaProducer;
  friend class KafkaConsumer;
  friend class KafkaPartitionConsumer;

  rd_kafka_topic_t *rkt_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAFKA_TOPIC_H_
