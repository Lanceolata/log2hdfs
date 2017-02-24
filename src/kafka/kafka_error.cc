// Copyright (c) 2017 Lanceolata

#include "kafka/kafka_error.h"

namespace log2hdfs {

const std::string ErrorToStr(ErrorCode err) {
  const char *es = rd_kafka_err2str(err);
  return std::string(es ? es : "");
}

const std::string ErrorToName(ErrorCode err) {
  const char *es = rd_kafka_err2name(err);
  return std::string(es ? es : "");
}

ErrorCode LastError() {
  return rd_kafka_last_error();
}

ErrorCode ErrnoToError(int errnox) {
  return rd_kafka_errno2err(errnox);
}

int KafkaErrno() {
  return rd_kafka_errno();
}

}   // namespace log2hdfs
