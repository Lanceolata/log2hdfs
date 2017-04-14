// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA_KAFKA_CONSUMER_H_
#define LOG2HDFS_KAFKA_KAFKA_CONSUMER_H_

#include <string>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

#ifdef __cplusplus
extern "C" {
#endif
#include "librdkafka/rdkafka.h"
#ifdef __cplusplus
}
#endif

#include "kafka/kafka_handle.h"

namespace log2hdfs {

class KafkaGlobalConf;
class KafkaTopicConf;
class KafkaHandle;
class KafkaTopic;

class KafkaConsumer {
 public:
  static std::shared_ptr<KafkaConsumer> Init(KafkaGlobalConf *conf);

  explicit KafkaConsumer(rd_kafka_t *rk): handle_(KafkaHandle::Init(rk)) {}

  ~KafkaConsumer();

  KafkaConsumer(const KafkaConsumer &other) = delete;
  KafkaConsumer &operator=(const KafkaConsumer &other) = delete;

  const std::string Name() const {
    return handle_->Name();
  }

  const std::string MemberId() const {
    return handle_->MemberId();
  }

  int Poll(int timeout_ms = 0) {
    return handle_->Poll(timeout_ms);
  }

  std::shared_ptr<KafkaTopic> CreateTopic(const std::string &topic,
                                          KafkaTopicConf *conf);

 private:
  std::mutex mutex_;
  std::shared_ptr<KafkaHandle> handle_;
  std::unordered_map<std::string,
      std::shared_ptr<KafkaTopic> > topics_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAFKA_CONSUMER_H_
