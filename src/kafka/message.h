#ifndef LOG2HDFS_KAFKA_MESSAGE_H
#define LOG2HDFS_KAFKA_MESSAGE_H

#include <string>

#ifdef __cplusplus
extern "C" {
#endif
#include "rdkafka.h"
#ifdef __cplusplus
}
#endif

#include "topic.h"

namespace log2hdfs {

namespace kafka {

class Message {
 public:

  static std::unique_ptr<Message> Create(rd_kafka_message_t* message);

  virtual ~Message() {}

  virtual ErrorCode err() const = 0;

  virtual const std::string ErrStr() const = 0;

  virtual void *payload() const = 0;

  virtual size_t len() const = 0;

  virtual const std::string *key() const = 0;

  virtual size_t key_len() const = 0;

  virtual int64_t offset() const = 0;

  virtual MessageTimestamp timestamp() const = 0;

  virtual void *opaque() const = 0;

};

}   // namespace kafka

}   // namespace log2hdfs

#endif
