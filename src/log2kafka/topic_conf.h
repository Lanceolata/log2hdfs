// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_LOG2KAFKA_TOPIC_CONF_H_
#define LOG2HDFS_LOG2KAFKA_TOPIC_CONF_H_

#include <string>
#include <vector>
#include <memory>
#include <atomic>
#include "kafka/kafka_conf.h"

namespace log2hdfs {

class Section;
class TopicConf;

/**
 * Topic conf same variable
 */
class TopicConfContents {
 private:
  friend class TopicConf;

  TopicConfContents();

  TopicConfContents(const TopicConfContents& other);

  TopicConfContents& operator=(const TopicConfContents& other) = delete;

  bool Update(std::shared_ptr<Section> section);

  bool UpdateRuntime(std::shared_ptr<Section> section);

  std::unique_ptr<KafkaTopicConf> kafka_topic_conf_;
  time_t remedy_;

  /*
   * Update runtime
   */
  std::atomic<int> batch_num_;
  std::atomic<int> poll_timeout_;
  std::atomic<int> poll_messages_;
};

/**
 * Topic conf
 */
class TopicConf {
 public:
  /**
   * Static function update default conf
   * 
   * @param                     Ini configuration section
   * 
   * @returns True if update success, false otherwise.
   */
  static bool UpdataDefaultConf(std::shared_ptr<Section> section);

  /**
   * Static function to create TopicConf shared_ptr
   * 
   * @param topic               topic name
   * 
   * @returns std::shared_ptr<TopicConf> if init success,
   *          nullptr otherwise.
   */
  static std::shared_ptr<TopicConf> Init(const std::string& topic);

  /**
   * Constructor
   */
  explicit TopicConf(const std::string& topic):
      topic_(topic), contents_(DEFAULT_CONTENTS_) {}

  /**
   * Init topic conf
   */
  bool InitConf(std::shared_ptr<Section> section);

  /**
   * Update topic conf runtime.
   * 
   * Just can update 4 configurations
   * [batch.num, poll.timeout, poll.messages]
   */
  bool UpdateRuntime(std::shared_ptr<Section> section);

  const std::string& topic() const {
    return topic_;
  }

  const std::vector<std::string>& dirs() const {
    return dirs_;
  }

  std::unique_ptr<KafkaTopicConf> kafka_topic_conf() const {
    return contents_.kafka_topic_conf_->Copy();
  }

  time_t remedy() const {
    return contents_.remedy_;
  }

  int batch_num() const {
    return contents_.batch_num_.load();
  }

  int poll_timeout() const {
    return contents_.poll_timeout_.load();
  }

  int poll_messages() const {
    return contents_.poll_messages_.load();
  }

 private:
  static TopicConfContents DEFAULT_CONTENTS_;

  std::string topic_;
  std::vector<std::string> dirs_;
  TopicConfContents contents_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_LOG2KAFKA_TOPIC_CONF_H_
