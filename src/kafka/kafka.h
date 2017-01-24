// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA_KAFKA_H
#define LOG2HDFS_KAFKA_KAFKA_H

#include <string>

#ifdef __cplusplus
extern "C" {
#endif
#include "rdkafka.h"
#ifdef __cplusplus
}
#endif

namespace log2hdfs {

namespace kafka {

// ------------------------------------------------------------------
// Forward declarations

class Conf;
class Message;
class Topic;
class TopicPartition;
class Producer;
class Consumer;

/ mpleConsumeCb ------------------------------------------------------------------
// Error

typedef rd_kafka_resp_err_t ErrorCode;

extern const char *ErrToStr(ErrorCode err);



// ------------------------------------------------------------------
// Topic




// ------------------------------------------------------------------
// Conf

}   // namespace kafka

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAFKA_H
