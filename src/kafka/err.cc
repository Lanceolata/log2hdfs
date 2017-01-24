// Copyright (c) 2017 Lanceolata

#include <string>
// #include "kafka/kafka.h"
#include "kafka.h"

namespace log2hdfs {

namespace kafka {

std::string ErrToStr(ErrorCode err) {
  return std::string(rd_kafka_err2str((err)));
}

}   // namespace kafka

}   // namespace log2hdfs
