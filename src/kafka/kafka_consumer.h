// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA_KAFKA_CONSUMER_H_
#define LOG2HDFS_KAFKA_KAFKA_CONSUMER_H_

#include <string>
#include <vector>
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
class KafkaTopicConsumer;
class KafkaConsumeCb;

class KafkaConsumer {
 public:
  static std::shared_ptr<KafkaConsumer> Init(KafkaGlobalConf* conf,
                                             std::string* errstr);

  explicit KafkaConsumer(std::shared_ptr<KafkaHandle> handle):
      handle_(std::move(handle)) {}

  ~KafkaConsumer() {
    StopAllTopic();
  }

  KafkaConsumer(const KafkaConsumer& other) = delete;
  KafkaConsumer& operator=(const KafkaConsumer& other) = delete;

  const std::string Name() const {
    return handle_->Name();
  }

  const std::string MemberId() const {
    return handle_->MemberId();
  }

  std::shared_ptr<KafkaTopicConsumer> CreateTopicConsumer(
      const std::string& topic,
      KafkaTopicConf* conf,
      const std::vector<int32_t>& partitions,
      const std::vector<int64_t>& offsets,
      std::shared_ptr<KafkaConsumeCb> cb,
      std::string* errstr);

  void StartAllTopic();

  void StopAllTopic();

  bool StartTopic(const std::string& topic);

  bool StopTopic(const std::string& topic);

 private:
  std::shared_ptr<KafkaHandle> handle_;
  std::mutex mutex_;
  std::unordered_map<std::string,
      std::shared_ptr<KafkaTopicConsumer>> topics_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAFKA_CONSUMER_H_
