// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA_KAFKA_HANDLE_H_
#define LOG2HDFS_KAFKA_KAFKA_HANDLE_H_

#include <string>
#include <memory>

#ifdef __cplusplus
extern "C" {
#endif
#include "librdkafka/rdkafka.h"
#ifdef __cplusplus
}
#endif

namespace log2hdfs {

class KafkaProducer;
class KafkaConsumer;

/**
 * Base handle
 */
class KafkaHandle {
 public:
  /**
   * Static function to create a KafkaHandle shared_ptr.
   * 
   * @param rk          librdkafka client raw pointer
   * 
   * @return std::shared_ptr<KafkaHandle> if rk valid,
   * nullptr otherwise.
   */
  static std::shared_ptr<KafkaHandle> Init(rd_kafka_t* rk);

  /**
   * Constructor
   */
  explicit KafkaHandle(rd_kafka_t* rk): rk_(rk) {}

  /**
   * Destructor
   * 
   * If rk_ valid, destory rk_.
   */
  ~KafkaHandle() {
    if (rk_)
      rd_kafka_destroy(rk_);
  }

  KafkaHandle(const KafkaHandle& other) = delete;
  KafkaHandle& operator=(const KafkaHandle& other) = delete;

  /**
   * @return the name of the handle.
   */
  const std::string Name() const {
    return std::string(rk_ ? rd_kafka_name(rk_) : "");
  }

  /**
   * Returns the client's broker-assigned group member id.
   * 
   * This currently requires the high-level KafkaConsumer
   * 
   * @returns Last assigned member id, or empty string if not currently
   *          a group member.
   */
  const std::string MemberId() const;

  /**
   * Polls the provided kafka handle for events.
   * 
   * Events will trigger application provided callbacks to be called.
   * 
   * The timeout_ms argument specifies the maximum amount of time
   * (in milliseconds) that the call will block waiting for events.
   * For non-blocking calls, provide 0 as timeout_ms.
   * To wait indefinately for events, provide -1.
   * 
   * @remark  An application should make sure to call poll() at regular
   *          intervals to serve any queued callbacks waiting to be called.
   *
   * @warning This method MUST NOT be used with the RdKafka::KafkaConsumer,
   *          use its RdKafka::KafkaConsumer::consume() instead.
   *          
   * @returns the number of events served.
   */
  int Poll(int timeout_ms) {
    return rd_kafka_poll(rk_, timeout_ms);
  }

  /**
   * Returns the current out queue length.
   * 
   * The out queue contains messages and requests waiting to be sent to,
   * or acknowledged by, the broker.
   */
  int OutqLen() {
    return rd_kafka_outq_len(rk_);
  }

  /**
   * Polls the provided kafka handle for events until queue length
   * less than length.
   * 
   * @param length      queue max length
   * @param timeout_ms  poll timeout
   * 
   * @return events number
   */
  int PollOutq(int length, int timeout_ms) {
    int n = 0;
    while (rd_kafka_outq_len(rk_) > length) {
      n += rd_kafka_poll(rk_, timeout_ms);
    }
    return n;
  }

 private:
  friend class KafkaProducer;
  friend class KafkaConsumer;

  rd_kafka_t* rk_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAFKA_HANDLE_H_
