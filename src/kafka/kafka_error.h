// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA_KAFKA_ERROR_H_
#define LOG2HDFS_KAFKA_KAFKA_ERROR_H_

#include <string>

#ifdef __cplusplus
extern "C" {
#endif
#include "librdkafka/rdkafka.h"
#ifdef __cplusplus
}
#endif

namespace log2hdfs {

typedef rd_kafka_resp_err_t KafkaErrorCode;

extern const std::string KafkaErrorToStr(KafkaErrorCode err);

extern const std::string KafkaErrorToName(KafkaErrorCode err);

extern KafkaErrorCode KafkaLastError();

extern KafkaErrorCode KafkaErrnoToError(int errnox);

extern const std::string KafkaErrnoToStr(int errnox);


}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAFKA_ERROR_H_
