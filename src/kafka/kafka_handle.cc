// Copyright (c) 2017 Lanceolata

#include "kafka/kafka_handle.h"

namespace log2hdfs {

std::shared_ptr<Handle> Handle::Init(rd_kafka_t *rk) {
  if (!rk) {
    return nullptr;
  }
  return std::shared_ptr<Handle>(new Handle(rk));
}

const std::string Handle::MemberId() const {
  char *str = rd_kafka_memberid(rk_);
  std::string memberid = str ? str : "";
  if (str) {
    rd_kafka_mem_free(rk_, str);
  }
  return memberid;
}

}   // namespace log2hdfs
