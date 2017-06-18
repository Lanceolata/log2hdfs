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

/**
 * Kafka producer
 */
class KafkaProducer {
 public:
  /**
   * Static function to create a KafkaProducer shared_ptr.
   * 
   * @param conf                kafka global conf
   * @param errstr              err string to set
   * 
   * @returns std::shared_ptr<KafkaProducer>
   */
  static std::shared_ptr<KafkaProducer> Init(
      KafkaGlobalConf* conf, std::string* errstr);

  /**
   * Constructor
   * 
   * @param handle              KafkaHandle shared_ptr
   */
  explicit KafkaProducer(std::shared_ptr<KafkaHandle> handle):
      handle_(std::move(handle)) {}

  /**
   * Destructor
   * 
   * Clear all topic producers and Poll to produce.
   */
  ~KafkaProducer() {
    std::unique_lock<std::mutex> lock(mutex_);
    topics_.clear();
    lock.unlock();
    handle_->PollOutq(0, 2000);
  }

  KafkaProducer(const KafkaProducer& other) = delete;
  KafkaProducer& operator=(const KafkaProducer& other) = delete;

  /**
   * See KafkaHandle Name().
   */
  const std::string Name() const {
    return handle_->Name();
  }

  /**
   * See KafkaHandle MemberId().
   */
  const std::string MemberId() const {
    return handle_->MemberId();
  }

  /**
   * See KafkaHandle Poll().
   */
  int Poll(int timeout_ms) {
    return handle_->Poll(timeout_ms);
  }

  /**
   * See KafkaHandle PollOutq().
   */
  int PollOutq(int length, int timeout_ms) {
    return handle_->PollOutq(length, timeout_ms);
  }

  /**
   * Create kafka topic producer
   * 
   * @param topic               topic name to create
   * @param conf                topic configuration
   * @param errstr              error string to set
   * 
   * @returns std::shared_ptr<KafkaTopicProducer> if success;
   *          nullptr otherwise and errstr to set.
   */
  std::shared_ptr<KafkaTopicProducer> CreateTopicProducer(
      const std::string& topic,
      KafkaTopicConf* conf,
      std::string* errstr);

  /**
   * Get kafka topic producer
   * 
   * @param topic               topic name to get
   * 
   * @returns std::shared_ptr<KafkaTopicProducer> if get success;
   *          nullptr otherwise.
   */
  std::shared_ptr<KafkaTopicProducer> GetTopicProducer(
      const std::string& topic);

  /**
   * Remove kafka topic producer
   * 
   * @param topic               topic name to remove
   * 
   * @returns True if remove success; false otherwise.
   */
  bool RemoveTopicProducer(const std::string& topic);

 private:
  std::shared_ptr<KafkaHandle> handle_;
  std::mutex mutex_;
  std::unordered_map<std::string,
      std::shared_ptr<KafkaTopicProducer>> topics_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAFKA_PRODUCER_H_
