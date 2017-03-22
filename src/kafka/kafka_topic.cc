// Copyright (c) 2017 Lanceolata

#include "kafka/kafka_topic.h"

namespace log2hdfs {

std::shared_ptr<KafkaTopic> KafkaTopic::Init(rd_kafka_topic_t *rkt) {
  if (!rkt) {
    return nullptr;
  }
  return std::make_shared<KafkaTopic>(rkt);
}

}   // namespace log2hdfs
