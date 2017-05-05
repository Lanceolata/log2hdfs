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

class KafkaHandle {
 public:
  static std::shared_ptr<KafkaHandle> Init(rd_kafka_t* rk);

  explicit KafkaHandle(rd_kafka_t* rk): rk_(rk) {}

  ~KafkaHandle() {
    if (rk_)
      rd_kafka_destroy(rk_);
  }

  KafkaHandle(const KafkaHandle& other) = delete;
  KafkaHandle& operator=(const KafkaHandle& other) = delete;

  const std::string Name() const {
    return std::string(rk_ ? rd_kafka_name(rk_) : "");
  }

  const std::string MemberId() const;

  int Poll(int timeout_ms = 200) {
    return rd_kafka_poll(rk_, timeout_ms);
  }

  int OutqLen() {
    return rd_kafka_outq_len(rk_);
  }

  int PollOutq(int length = 0, int timeout_ms = 2000) {
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
