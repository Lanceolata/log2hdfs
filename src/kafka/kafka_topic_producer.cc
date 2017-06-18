// Copyright (c) 2017 Lanceolata

#include "kafka/kafka_topic_producer.h"

namespace log2hdfs {

std::shared_ptr<KafkaTopicProducer> KafkaTopicProducer::Init(
    std::shared_ptr<KafkaHandle> handle,
    std::shared_ptr<KafkaTopic> topic) {
  if (!handle || !topic)
    return nullptr;

  return std::make_shared<KafkaTopicProducer>(
             std::move(handle), std::move(topic));
}

}   // namespace log2hdfs
