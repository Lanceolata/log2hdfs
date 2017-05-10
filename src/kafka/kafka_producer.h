// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA_KAFKA_PRODUCER_H_
#define LOG2HDFS_KAFKA_KAFKA_PRODUCER_H_

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
class KafkaTopicProducer;

class KafkaProducer {
 public:
  static std::shared_ptr<KafkaProducer> Init(
      KafkaGlobalConf* conf, std::string* errstr);

  explicit KafkaProducer(std::shared_ptr<KafkaHandle> handle):
      handle_(std::move(handle)) {}

  ~KafkaProducer() {
    std::unique_lock<std::mutex> guard(mutex_);
    topics_.clear();
    guard.unlock();
    handle_->PollOutq(0, 2000);
  }

  KafkaProducer(const KafkaProducer& other) = delete;
  KafkaProducer& operator=(const KafkaProducer& other) = delete;

  const std::string Name() const {
    return handle_->Name();
  }

  const std::string MemberId() const {
    return handle_->MemberId();
  }

  int Poll(int timeout_ms) {
    return handle_->Poll(timeout_ms);
  }

  int PollOutq(int length, int timeout_ms) {
    return handle_->PollOutq(length, timeout_ms);
  }

  std::shared_ptr<KafkaTopicProducer> CreateTopicProducer(
      const std::string& topic, KafkaTopicConf* conf, std::string* errstr);

  // Return nullptr if topic not found.
  std::shared_ptr<KafkaTopicProducer> GetTopicProducer(
      const std::string& topic);

  bool RemoveTopicProducer(const std::string& topic);

 private:
  std::shared_ptr<KafkaHandle> handle_;
  std::mutex mutex_;
  std::unordered_map<std::string,
      std::shared_ptr<KafkaTopicProducer>> topics_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAFKA_PRODUCER_H_
