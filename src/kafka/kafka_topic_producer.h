// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA_KAFKA_TOPIC_PRODUCER_H_
#define LOG2HDFS_KAFKA_KAFKA_TOPIC_PRODUCER_H_

#include <string>
#include <memory>

#ifdef __cplusplus
extern "C" {
#endif
#include "librdkafka/rdkafka.h"
#ifdef __cplusplus
}
#endif

#include "kafka/kafka_handle.h"
#include "kafka/kafka_topic.h"

namespace log2hdfs {

class KafkaTopicProducer {
 public:
  static std::shared_ptr<KafkaTopicProducer> Init(
      std::shared_ptr<KafkaHandle> handle, std::shared_ptr<KafkaTopic> topic);

  KafkaTopicProducer(std::shared_ptr<KafkaHandle> handle,
                     std::shared_ptr<KafkaTopic> topic):
      handle_(std::move(handle)), topic_(std::move(topic)) {}

  KafkaTopicProducer(const KafkaTopicProducer& other) = delete;
  KafkaTopicProducer &operator=(const KafkaTopicProducer& other) = delete;

  const std::string Name() const {
    return topic_->Name();
  }

  int Poll(int timeout_ms = 200) {
    return handle_->Poll(timeout_ms);
  }

  int ProduceMessage(const std::string& payload) {
    if (payload.empty())
      return -2;

    void *p = const_cast<char *>(payload.c_str());
    return rd_kafka_produce(topic_->rkt_, RD_KAFKA_PARTITION_UA,
                            RD_KAFKA_MSG_F_COPY, p, payload.size(),
                            NULL, 0, NULL);
  }

 private:
  std::shared_ptr<KafkaHandle> handle_;
  std::shared_ptr<KafkaTopic> topic_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAFKA_TOPIC_PRODUCER_H_
