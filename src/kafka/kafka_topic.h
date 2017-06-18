// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA_KAFKA_TOPIC_H_
#define LOG2HDFS_KAFKA_KAFKA_TOPIC_H_

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

class KafkaTopicProducer;
class KafkaTopicConsumer;

/**
 * kafka topic
 */
class KafkaTopic {
 public:
  /**
   * Static function to create a KafkaTopic shared_ptr
   * 
   * @param rkt                 librdkafka topic raw pointer
   * 
   * @returns std::shared_ptr<KafkaTopic> if rkt valid,
   *          nullptr otherwise.
   */
  static std::shared_ptr<KafkaTopic> Init(rd_kafka_topic_t* rkt);

  /**
   * Constructor
   */
  explicit KafkaTopic(rd_kafka_topic_t* rkt): rkt_(rkt) {}

  /**
   * Destructor
   * 
   * If rkt_ valid, destory rkt_.
   */
  ~KafkaTopic() {
    if (rkt_)
      rd_kafka_topic_destroy(rkt_);
  }

  KafkaTopic(const KafkaTopic& other) = delete;
  KafkaTopic& operator=(const KafkaTopic& other) = delete;

  /**
   * @returns the name of the topic.
   */
  const std::string Name() const {
    const char* tn = rd_kafka_topic_name(rkt_);
    return std::string(tn ? tn : "");
  }

 private:
  friend class KafkaTopicProducer;
  friend class KafkaTopicConsumer;

  rd_kafka_topic_t* rkt_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAFKA_TOPIC_H_
