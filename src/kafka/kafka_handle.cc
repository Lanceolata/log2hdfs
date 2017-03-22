// Copyright (c) 2017 Lanceolata

#include "kafka/kafka_handle.h"

namespace log2hdfs {

std::shared_ptr<KafkaHandle> KafkaHandle::Init(rd_kafka_t *rk) {
  if (!rk) {
    return nullptr;
  }
  return std::make_shared<KafkaHandle>(rk);
}

const std::string KafkaHandle::MemberId() const {
  char *str = rd_kafka_memberid(rk_);
  std::string memberid = str ? str : "";
  if (str) {
    rd_kafka_mem_free(rk_, str);
  }
  return memberid;
}

}   // namespace log2hdfs
