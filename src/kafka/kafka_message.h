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

/**
 * Message timestamp object
 * 
 * Represents the number of milliseconds since the epoch (UTC).
 * 
 * The MessageTimestampType dictates the timestamp type or origin.
 * 
 * @remark Requires Apache Kafka broker version >= 0.10.0
 */
class MessageTimestamp {
 public:
  enum MessageTimestampType {
    MSG_TIMESTAMP_NOT_AVAILABLE,    /**< Timestamp not available */
    MSG_TIMESTAMP_CREATE_TIME,      /**< Message creation time (source) */
    MSG_TIMESTAMP_LOG_APPEND_TIME   /**< Message log append time (broker) */
  };

  MessageTimestampType type;        /**< Timestamp type */
  int64_t timestamp;                /**< Milliseconds since epoch (UTC). */
};

/**
 * Kafka message object
 * 
 * This object represents either a single consumed or produced message,
 * or an event (\p err() is set).
 * 
 * An application must check KafkaMessage::Error() to see if the
 * object is a proper message or an error event.
 */
class KafkaMessage {
 public:
  /**
   * Static function to create a KafkaGlobalConf unique_ptr.
   * 
   * @returns std::unique_ptr<KafkaMessage>
   */
  static std::unique_ptr<KafkaMessage> Init(rd_kafka_message_t* rkmessage);

  /**
   * Constructor
   * 
   * @param rkmessage           librdkafka kafka message raw pointer
   */
  explicit KafkaMessage(rd_kafka_message_t* rkmessage):
      rkmessage_(rkmessage) {}

  /**
   * Destructor
   * 
   * If rkmessage_ valid, destroy rkmessage_.
   */
  ~KafkaMessage() {
    if (rkmessage_)
      rd_kafka_message_destroy(rkmessage_);
  }

  KafkaMessage(const KafkaMessage& other) = delete;
  KafkaMessage& operator=(const KafkaMessage& other) = delete;

  const std::string TopicName() const {
    if (rkmessage_->rkt) {
      return rd_kafka_topic_name(rkmessage_->rkt);
    } else {
      return "";
    }
  }

  /**
   * @returns The error string if object represent an error event,
   *          else an empty string.
   */
  const std::string ErrStr() const {
    return KafkaErrorToStr(rkmessage_->err);
  }

  /**
   * @returns The error code if object represents an error event, else 0.
   */
  KafkaErrorCode Error() const {
    return rkmessage_->err;
  }

  /**
   * @returns Partition (if applicable)
   */
  int32_t Partition() const {
    return rkmessage_->partition;
  }

  /**
   * @returns Message payload (if applicable)
   */
  void* Payload() const {
    return rkmessage_->payload;
  }

  /**
   * @returns Message payload length (if applicable)
   */
  size_t Len() const {
    return rkmessage_->len;
  }

  /**
   * @returns Message key as string (if applicable)
   */
  void* Key() const {
    return rkmessage_->key;
  }

  /**
   * @returns Message key's binary length (if applicable)
   */
  size_t KeyLen() const {
    return rkmessage_->key_len;
  }

  /**
   * @returns Message or error offset (if applicable)
   */
  int64_t Offset() const {
    return rkmessage_->offset;
  }

  /**
   * @returns Message timestamp (if applicable)
   */
  MessageTimestamp Timestamp() const;

  /**
   * @returns The msg_opaque as provided to KafkaProducer::produce()
   */
  const void* MsgOpaque() const {
    return rkmessage_->_private;
  }

 private:
  rd_kafka_message_t* rkmessage_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAFKA_MESSAGE_H_
