// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_TOPIC_CONF_H_
#define LOG2HDFS_KAFKA2HDFS_TOPIC_CONF_H_

#include <string>
#include <set>
#include <vector>
#include <memory>
#include <mutex>
#include <atomic>
#include "kafka/kafka_conf.h"
#include "kafka2hdfs/consume_callback.h"
#include "kafka2hdfs/log_format.h"
#include "kafka2hdfs/path_format.h"
#include "kafka2hdfs/upload.h"

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

  std::string consume_dir_;
  std::string compress_dir_;
  std::string upload_dir_;

  std::unique_ptr<KafkaTopicConf> kafka_topic_conf_;

  LogFormat::Type log_format_;
  PathFormat::Type path_format_;
  ConsumeCallback::Type consume_type_;

  Upload::Type file_format_;
  size_t parallel_;
  std::string compress_lzo_;
  std::string compress_orc_;
  std::string compress_mv_;

  // flow variable thread safe
  std::atomic<int> consume_interval_;
  std::atomic<int> complete_interval_;
  std::atomic<long> complete_maxsize_;
  std::atomic<int> complete_maxseconds_;
  std::atomic<int> upload_interval_;
};

class TopicConf {
 public:
  // Must call before new TopicConf.
  static bool UpdataDefaultConf(std::shared_ptr<Section> section);

  static std::shared_ptr<TopicConf> Init(const std::string& section);

  TopicConf(const std::string& section):
      section_(section), contents_(DEFAULT_CONTENTS_) {}

  bool InitConf(std::shared_ptr<Section> section);

  // This function runtime update conf safe.
  bool UpdateRuntime(std::shared_ptr<Section> section);

  const std::string& section() const {
    return section_;
  }

  const std::string& consume_dir() const {
    return contents_.consume_dir_;
  }

  const std::string& compress_dir() const {
    return contents_.compress_dir_;
  }

  const std::string& upload_dir() const {
    return contents_.upload_dir_;
  }

  const std::vector<std::string>& topics() const {
    return topics_;
  }

  const std::vector<std::vector<int32_t>>& partitions() const {
    return partitions_;
  }

  const std::vector<std::vector<int64_t>>& offsets() const {
    return offsets_;
  }

  std::string hdfs_path() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return hdfs_path_;
  }

  std::unique_ptr<KafkaTopicConf> kafka_topic_conf() const {
    return contents_.kafka_topic_conf_->Copy();
  }

  LogFormat::Type log_format() const {
    return contents_.log_format_;
  }

  PathFormat::Type path_format() const {
    return contents_.path_format_;
  }

  ConsumeCallback::Type consume_type() const {
    return contents_.consume_type_;
  }

  Upload::Type file_format() const {
    return contents_.file_format_;
  }

  size_t parallel() const {
    return contents_.parallel_;
  }

  const std::string& compress_lzo() const {
    return contents_.compress_lzo_;
  }

  const std::string& compress_orc() const {
    return contents_.compress_orc_;
  }

  const std::string& compress_mv() const {
    return contents_.compress_mv_;
  }

  int consume_interval() const {
    return contents_.consume_interval_.load();
  }

  int complete_interval() const {
    return contents_.complete_interval_.load();
  }

  long complete_maxsize() const {
    return contents_.complete_maxsize_.load();
  }

  int complete_maxseconds() const {
    return contents_.complete_maxseconds_.load();
  }

  int upload_interval() const {
    return contents_.upload_interval_.load();
  }

 private:
  static TopicConfContents DEFAULT_CONTENTS_;

  std::string section_;
  std::vector<std::string> topics_;
  std::vector<std::vector<int32_t>> partitions_;
  std::vector<std::vector<int64_t>> offsets_;
  std::string hdfs_path_;
  TopicConfContents contents_;
  mutable std::mutex mutex_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_TOPIC_CONF_H_
