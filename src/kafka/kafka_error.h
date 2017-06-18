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

/**
 * Returns a human readable representation of a kafka error.
 * 
 * @param err                   error code to translate
 * 
 * @return kafka error.
 */
extern const std::string KafkaErrorToStr(KafkaErrorCode err);

/**
 * Returns the error code name (enum name).
 * 
 * @param err                   error code to translate
 * 
 * @return code name
 */
extern const std::string KafkaErrorToName(KafkaErrorCode err);

/**
 * Returns the last error code generated by a legacy API call
 * in the current thread.
 * 
 * @return error code
 */
extern KafkaErrorCode KafkaLastError();

/**
 * Converts the system errno value errnox to a rd_kafka_resp_err_t
 * error code upon failure.
 * 
 * @param errnox                system errno value to convert
 * 
 * @return error code
 */
extern KafkaErrorCode KafkaErrnoToError(int errnox);

/**
 * Converts the system errno value errnox to a human readable
 * representation of a kafka error.
 * 
 * @param errnox                system errno value to convert
 * 
 * @return error string
 */
extern const std::string KafkaErrnoToStr(int errnox);

/**
 * Returns the thread-local system errno.
 * 
 * @return system errno
 */
extern int KafkaErrno();

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAFKA_ERROR_H_
