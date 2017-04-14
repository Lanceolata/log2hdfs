// Copyright (c) 2017 Lanceolata

#include "kafka/kafka_topic_handle.h"

namespace log2hdfs {

std::shared_ptr<KafkaTopicHandle> KafkaTopicHandle::Init(
    rd_kafka_topic_t *rkt) {
  if (!rkt) {
    return nullptr;
  }
  return std::make_shared<KafkaTopicHandle>(rkt);
}

}   // namespace log2hdfs
