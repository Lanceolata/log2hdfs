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

typedef rd_kafka_resp_err_t ErrorCode;

extern const std::string ErrorToStr(ErrorCode err);

extern const std::string ErrorToName(ErrorCode err);

extern ErrorCode LastError();

extern ErrorCode ErrnoToError(int errnox);

extern int KafkaErrno();

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAFKA_ERROR_H_
