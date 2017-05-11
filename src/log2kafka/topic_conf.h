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

  // Update runtime
  std::atomic<int> batch_num_;
  std::atomic<int> poll_timeout_;
  std::atomic<int> poll_messages_;
};

class TopicConf {
 public:
  // Must call before new TopicConf.
  static bool UpdataDefaultConf(std::shared_ptr<Section> section);

  static std::shared_ptr<TopicConf> Init(const std::string& topic);

  explicit TopicConf(const std::string& topic):
      topic_(topic), contents_(DEFAULT_CONTENTS_) {}

  bool InitConf(std::shared_ptr<Section> section);

  // This function runtime update conf safe.
  // Just can update 3 configuration [batch.num,poll.timeout,poll.messages]
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
