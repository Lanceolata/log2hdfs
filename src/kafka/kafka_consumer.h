// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA_KAFKA_CONSUMER_H_
#define LOG2HDFS_KAFKA_KAFKA_CONSUMER_H_

#include <string>
#include <memory>
#include <mutex>
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
class KafkaPartitionConsumer;
class KafkaPartitionConsumerCb;

class KafkaConsumer {
 public:
  static std::shared_ptr<KafkaConsumer> Init(KafkaGlobalConf *conf);

  explicit KafkaConsumer(rd_kafka_t *rk): handle_(KafkaHandle::Init(rk)) {}

  KafkaConsumer(const KafkaConsumer &other) = delete;
  KafkaConsumer &operator=(const KafkaConsumer &other) = delete;

  ~KafkaConsumer() {}

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

  std::shared_ptr<KafkaPartitionConsumer> CreatePartitionConsumer(
      const std::string &topic, int32_t partition,
      int64_t offset, std::shared_ptr<KafkaPartitionConsumerCb> cb);

  bool StartTopic(const std::string &topic);

  void StopAll(int milli = 5000);

 private:
  std::mutex mutex_;
  std::shared_ptr<KafkaHandle> handle_;
  std::unordered_map<std::string,
      std::shared_ptr<KafkaTopic> > topics_;
  std::unordered_multimap<std::string,
      std::shared_ptr<KafkaPartitionConsumer> > consumers_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAFKA_CONSUMER_H_
