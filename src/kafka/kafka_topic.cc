// Copyright (c) 2017 Lanceolata

#include "kafka/kafka_topic.h"

namespace log2hdfs {

std::shared_ptr<Topic> Topic::Init(rd_kafka_topic_t *rkt) {
  if (!rkt) {
    return nullptr;
  }
  return std::shared_ptr<Topic>(new Topic(rkt));
}

}   // namespace log2hdfs
