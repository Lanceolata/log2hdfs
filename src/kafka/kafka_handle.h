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

class Producer;
class Consumer;

class Handle {
 public:
  static std::shared_ptr<Handle> Init(rd_kafka_t *rk);

  Handle(const Handle &h) = delete;
  Handle &operator=(const Handle &h) = delete;

  ~Handle() {
    if (rk_) {
      rd_kafka_destroy(rk_);
    }
  }

  const std::string Name() const {
    return std::string(rd_kafka_name(rk_));
  }

  const std::string MemberId() const;

  int Poll(int timeout_ms) {
    return rd_kafka_poll(rk_, timeout_ms);
  }

  int OutqLen() {
    return rd_kafka_outq_len(rk_);
  }

 private:
  friend class Producer;
  friend class Consumer;

  explicit Handle(rd_kafka_t *rk): rk_(rk) {}

  rd_kafka_t *rk_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAFKA_HANDLE_H_
