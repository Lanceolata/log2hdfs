// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA_KAFKA_MESSAGE_H_
#define LOG2HDFS_KAFKA_KAFKA_MESSAGE_H_

#include <string>
#include <memory>

#ifdef __cplusplus
extern "C" {
#endif
#include "librdkafka/rdkafka.h"
#ifdef __cplusplus
}
#endif

#include "kafka/kafka_error.h"

namespace log2hdfs {

struct MessageTimestamp {
 public:
  enum MessageTimestampType {
    MSG_TIMESTAMP_NOT_AVAILABLE,    // Timestamp not available
    MSG_TIMESTAMP_CREATE_TIME,      // Message creation time (source)
    MSG_TIMESTAMP_LOG_APPEND_TIME   // Message log append time (broker)
  };

  MessageTimestampType type;
  int64_t timestamp;
};

class Message {
 public:
  static std::unique_ptr<Message> Init(rd_kafka_message_t *rkmessage);

  Message(const Message &m) = delete;
  Message &operator=(const Message &m) = delete;

  ~Message() {
    if (rkmessage_) {
      rd_kafka_message_destroy(rkmessage_);
    }
  }

  std::string TopicName() const {
    if (rkmessage_->rkt) {
      return rd_kafka_topic_name(rkmessage_->rkt);
    } else {
      return "";
    }
  }

  const std::string ErrStr() const {
    return ErrorToStr(rkmessage_->err);
  }

  ErrorCode Error() const {
    return rkmessage_->err;
  }

  int32_t Partition() const {
    return rkmessage_->partition;
  }

  void *Payload() const {
    return rkmessage_->payload;
  }

  size_t Len() const {
    return rkmessage_->len;
  }

  void *Key() const {
    return rkmessage_->key;
  }

  size_t KeyLen() const {
    return rkmessage_->key_len;
  }

  int64_t Offset() const {
    return rkmessage_->offset;
  }

  MessageTimestamp Timestamp() const;

  const void *MsgOpaque() const {
    return rkmessage_->_private;
  }

 private:
  explicit Message(rd_kafka_message_t *rkmessage):
      rkmessage_(rkmessage) {}

  rd_kafka_message_t *rkmessage_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAFKA_MESSAGE_H_