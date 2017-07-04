// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_TOPIC_CONF_H_
#define LOG2HDFS_KAFKA2HDFS_TOPIC_CONF_H_

#include <string>
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

/**
 * Topic conf contents
 */
class TopicConfContents {
 private:
  friend class TopicConf;

  /**
   * Constructor
   */
  TopicConfContents();

  /**
   * Copy constructor
   */
  TopicConfContents(const TopicConfContents& other);

  TopicConfContents& operator=(const TopicConfContents& other) = delete;

  /**
   * Update configurations
   * 
   * @param section             configure section
   * 
   * @returns True if update success, false otherwise.
   */
  bool Update(std::shared_ptr<Section> section);

  /**
   * Update configurations runtime
   * 
   * @param section             configure section
   * 
   * @returns True if update success, false otherwise.
   */
  bool UpdateRuntime(std::shared_ptr<Section> section);

  std::string GetCompressLzo() {
    std::lock_guard<std::mutex> lock(mutex_);
    return compress_lzo_;
  }

  std::string GetCompressOrc() {
    std::lock_guard<std::mutex> lock(mutex_);
    return compress_orc_;
  }

  std::string GetCompressMv() {
    std::lock_guard<std::mutex> lock(mutex_);
    return compress_mv_;
  }

  std::string root_dir_;
  std::unique_ptr<KafkaTopicConf> kafka_topic_conf_;

  LogFormat::Type log_format_;
  PathFormat::Type path_format_;
  ConsumeCallback::Type consume_type_;
  Upload::Type upload_type_;
  size_t parallel_;

  // flow variable thread safe
  std::string compress_lzo_;
  std::string compress_orc_;
  std::string compress_mv_;

  std::atomic<int> consume_interval_;
  std::atomic<int> complete_interval_;
  std::atomic<long> complete_maxsize_;
  std::atomic<int> retention_seconds_;
  std::atomic<int> upload_interval_;

  mutable std::mutex mutex_;
};

class TopicConf {
 public:
  /**
   * Update default configuration
   * 
   * Must call before create TopicConf.
   * 
   * @param section             configure section
   * 
   * @return True if update success, false otherwise.
   */
  static bool UpdataDefaultConf(std::shared_ptr<Section> section);

  /**
   * Static function create TopicConf shared_ptr.
   */
  static std::shared_ptr<TopicConf> Init(const std::string& topic);

  /**
   * Constructor
   */
  explicit TopicConf(const std::string& topic):
      topic_(topic), contents_(DEFAULT_CONTENTS_) {}

  /**
   * Init topic conf.
   */
  bool InitConf(std::shared_ptr<Section> section);

  /**
   * Update topic conf runtime.
   * 
   * This function runtime update conf safe.
   */
  bool UpdateRuntime(std::shared_ptr<Section> section);

  const std::string& topic() const {
    return topic_;
  }

  const std::string& consume_dir() const {
    return consume_dir_;
  }

  const std::string& compress_dir() const {
    return compress_dir_;
  }

  const std::string& upload_dir() const {
    return upload_dir_;
  }

  const std::vector<int32_t>& partitions() const {
    return partitions_;
  }

  const std::vector<int64_t>& offsets() const {
    return offsets_;
  }

  std::string hdfs_path() const {
    std::lock_guard<std::mutex> lock(mutex_);
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

  Upload::Type upload_type() const {
    return contents_.upload_type_;
  }

  size_t parallel() const {
    return contents_.parallel_;
  }

  std::string compress_lzo() const {
    return contents_.GetCompressLzo();
  }

  std::string compress_orc() const {
    return contents_.GetCompressOrc();
  }

  std::string compress_mv() const {
    return contents_.GetCompressMv();
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

  int retention_seconds() const {
    return contents_.retention_seconds_.load();
  }

  int upload_interval() const {
    return contents_.upload_interval_.load();
  }

 private:
  static TopicConfContents DEFAULT_CONTENTS_;

  std::string topic_;
  std::vector<int32_t> partitions_;
  std::vector<int64_t> offsets_;
  std::string consume_dir_;
  std::string compress_dir_;
  std::string upload_dir_;
  std::string hdfs_path_;
  TopicConfContents contents_;
  mutable std::mutex mutex_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_TOPIC_CONF_H_
