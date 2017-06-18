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

/**
 * Kafka topic producer
 */
class KafkaTopicProducer {
 public:
  /**
   * Static function to create a KafkaTopicProducer shared_ptr.
   * 
   * @param handle              KafkaHandle shared_ptr
   * @param topic               KafkaTopic shared_ptr
   * 
   * @returns std::shared_ptr<KafkaTopicProducer>
   */
  static std::shared_ptr<KafkaTopicProducer> Init(
      std::shared_ptr<KafkaHandle> handle,
      std::shared_ptr<KafkaTopic> topic);

  /**
   * Constructor
   * 
   * @param handle              KafkaHandle shared_ptr
   * @param topic               KafkaTopic shared_ptr
   */
  KafkaTopicProducer(std::shared_ptr<KafkaHandle> handle,
                     std::shared_ptr<KafkaTopic> topic):
      handle_(std::move(handle)), topic_(std::move(topic)) {}

  KafkaTopicProducer(const KafkaTopicProducer& other) = delete;
  KafkaTopicProducer &operator=(const KafkaTopicProducer& other) = delete;

  /**
   * See KafkaTopic Name().
   */
  const std::string Name() const {
    return topic_->Name();
  }

  /**
   * See KafkaHandle Poll().
   */
  int Poll(int timeout_ms) {
    return handle_->Poll(timeout_ms);
  }

  /**
   * Produce message payload.
   * 
   * @param payload             message to produce
   * 
   * @return 0 if produce payload success, 1 on error, errno is
   *         set appropriately. 2 on empty payload.
   */
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
