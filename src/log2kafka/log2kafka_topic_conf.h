// Copyright (c) 2017 Lanceolata

#ifndef LOG2KAFKA_LOG2KAFKA_TOPIC_CONF_H_
#define LOG2KAFKA_LOG2KAFKA_TOPIC_CONF_H_

#include <string>
#include <vector>
#include <memory>
#include <atomic>
#include "kafka/kafka_conf.h"

namespace log2hdfs {

class Section;
class Log2kafkaTopicConf;

class LkTopicConfContents {
 private:
  friend class Log2kafkaTopicConf;

  LkTopicConfContents();

  LkTopicConfContents(const LkTopicConfContents& other);

  LkTopicConfContents& operator=(const LkTopicConfContents& other) = delete;

  bool Update(std::shared_ptr<Section> section);

  bool UpdateRuntime(std::shared_ptr<Section> section);

  std::unique_ptr<KafkaTopicConf> kafka_topic_conf_;

  std::atomic<int> produce_batch_num_;
  std::atomic<int> poll_timeout_;
  std::atomic<int> poll_messages_;
  time_t remedy_expire_;
};

class Log2kafkaTopicConf {
 public:
  // Call before create Log2kafkaTopicConf.
  static bool UpdataDefaultConf(std::shared_ptr<Section> section);

  static std::shared_ptr<Log2kafkaTopicConf> Init(
      const std::string& section_name);

  explicit Log2kafkaTopicConf(const std::string& topic_name):
      topic_name_(topic_name), contents_(DEFAULT_CONTENTS_) {}

  bool InitConf(std::shared_ptr<Section> section);

  bool UpdateRuntime(std::shared_ptr<Section> section);

  const std::string& topic_name() const {
    return topic_name_;
  }

  const std::vector<std::string>& dir_path() const {
    return dir_paths_;
  }

  std::unique_ptr<KafkaTopicConf> kafka_topic_conf() const {
    return contents_.kafka_topic_conf_->Copy();
  }

  int produce_batch_num() const {
    return contents_.produce_batch_num_.load();
  }

  int poll_timeout() const {
    return contents_.poll_timeout_.load();
  }

  int poll_messages() const {
    return contents_.poll_messages_.load();
  }

  time_t remedy_expire() const {
    return contents_.remedy_expire_;
  }

 private:
  static LkTopicConfContents DEFAULT_CONTENTS_;

  std::string topic_name_;
  std::vector<std::string> dir_paths_;
  LkTopicConfContents contents_;
};

}   // namespace log2hdfs

#endif  // LOG2KAFKA_LOG2KAFKA_TOPIC_CONF_H_
