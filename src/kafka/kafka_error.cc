// Copyright (c) 2017 Lanceolata

#include "kafka/kafka_error.h"

namespace log2hdfs {

const std::string KafkaErrorToStr(KafkaErrorCode err) {
  const char *es = rd_kafka_err2str(err);
  return std::string(es ? es : "");
}

const std::string KafkaErrorToName(KafkaErrorCode err) {
  const char *es = rd_kafka_err2name(err);
  return std::string(es ? es : "");
}

KafkaErrorCode KafkaLastError() {
  return rd_kafka_last_error();
}

KafkaErrorCode KafkaErrnoToError(int errnox) {
  return rd_kafka_errno2err(errnox);
}

const std::string KafkaErrnoToStr(int errnox) {
  KafkaErrorCode code = rd_kafka_errno2err(errnox);
  return KafkaErrorToStr(code);
}

int KafkaErrno() {
  return rd_kafka_errno();
}

}   // namespace log2hdfs
