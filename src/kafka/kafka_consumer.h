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

/**
 * Kafka consumer
 */
class KafkaConsumer {
 public:
  /**
   * Static function to create a KafkaConsumer shared_ptr.
   * 
   * @param conf                kafka global conf
   * @param errstr              err string to set
   * 
   * @returns std::shared_ptr<KafkaConsumer>
   */
  static std::shared_ptr<KafkaConsumer> Init(
      KafkaGlobalConf* conf, std::string* errstr);

  /**
   * Constructor
   * 
   * @param handle              KafkaHandle shared_ptr
   */
  explicit KafkaConsumer(std::shared_ptr<KafkaHandle> handle):
      handle_(std::move(handle)) {}

  /**
   * Destructor
   * 
   * Stop All consume threads and clear topics.
   */
  ~KafkaConsumer() {
    StopAllTopic();
  }

  KafkaConsumer(const KafkaConsumer& other) = delete;
  KafkaConsumer& operator=(const KafkaConsumer& other) = delete;

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
   * Create topic consumer
   * 
   * @param topic               topic name
   * @param conf                kafka topic conf
   * @param partitions          topic partitions
   * @param offsets             partition offsets
   * @param cb                  KafkaConsumeCb callback
   * @param errstr              err string to set
   * 
   * @returns std::shared_ptr<KafkaTopicConsumer> if success;
   *          nullptr otherwise.
   *          
   * If topic consumer already created, return nullptr.
   */
  std::shared_ptr<KafkaTopicConsumer> CreateTopicConsumer(
      const std::string& topic,
      KafkaTopicConf* conf,
      const std::vector<int32_t>& partitions,
      const std::vector<int64_t>& offsets,
      std::shared_ptr<KafkaConsumeCb> cb,
      std::string* errstr);

  /**
   * Start all topic consume.
   */
  void StartAllTopic();

  /**
   * Stop all topic consume.
   */
  void StopAllTopic();

  /**
   * Start topic consume.
   * 
   * @param topic               topic name
   * 
   * @returns True if success; false otherwise.
   */
  bool StartTopic(const std::string& topic);

  /**
   * Stop topic consume.
   * 
   * @param topic               topic name
   * 
   * @returns True if success; false otherwise.
   */
  bool StopTopic(const std::string& topic);

 private:
  std::shared_ptr<KafkaHandle> handle_;
  mutable std::mutex mutex_;
  std::unordered_map<std::string,
      std::shared_ptr<KafkaTopicConsumer>> topics_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAFKA_CONSUMER_H_
