// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_KAFKA2HDFS_TOPIC_CONF_H_
#define LOG2HDFS_KAFKA2HDFS_KAFKA2HDFS_TOPIC_CONF_H_

#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <atomic>
#include <unordered_map>
#include "kafka2hdfs/log_format.h"
#include "kafka2hdfs/consumer_callback.h"
#include "kafka2hdfs/compress.h"
#include "kafka2hdfs/upload.h"
#include "kafka/kafka_conf.h"

namespace log2hdfs {

class Section;

class K2HTopicConfContents {
 public:
  K2HTopicConfContents();

  K2HTopicConfContents(const K2HTopicConfContents &other);

  ~K2HTopicConfContents() {}

  K2HTopicConfContents &operator=(const K2HTopicConfContents &other) = delete;

  // Default configuration
  std::unique_ptr<KafkaTopicConf> kafka_topic_conf;
  std::shared_ptr<std::string> rootdir;

  // Default consume configuration
  ConsumeCallback::Type consume_type;
  int64_t consume_offset;
  std::atomic<int> consume_interval;
  std::atomic<long long> consume_maxsize;
  std::atomic<int> consume_complete_interval;

  // Default compress configuration
  Compress::Type compress_type;
  std::atomic<int> compress_interval;
  std::shared_ptr<std::string> compress_lzo;

  // Default upload configuration
  Upload::Type upload_type;
  std::shared_ptr<std::string> upload_put;
  std::shared_ptr<std::string> upload_append;
  std::shared_ptr<std::string> upload_lzoindex;
};


class Kafka2hdfsTopicConf {
 public:
  static bool UpdateDefaultConf(std::shared_ptr<Section> section);

  static std::shared_ptr<Kafka2hdfsTopicConf> Init(
      const std::string &section_name,
      std::shared_ptr<Section> section);

  Kafka2hdfsTopicConf(const std::string &section_name,
                      std::shared_ptr<Section> section);

  ~Kafka2hdfsTopicConf() {}

  // update conf runtime
  // only update:
  // consume_interval consume_complete_interval compress_maxsize
  // compress_interval hdfs_path_format_ delay_interval_
  // hdfs_path_format_delay_
  //bool UpdateConf(std::shared_ptr<Section> section);

  std::shared_ptr<std::string> section_name() {
    return section_name_;
  }

  std::shared_ptr<std::string> consume_dir() {
    return consume_dir_;
  }

  std::shared_ptr<std::string> compress_dir() {
    return compress_dir_;
  }

  std::shared_ptr<std::string> upload_dir() {
    return upload_dir_;
  }

  std::shared_ptr<std::string> hdfs_path_format() {
    std::lock_guard<std::mutex> guard(mutex_);
    return hdfs_path_format_;
  }

  std::shared_ptr<std::string> cvt_move() {
    return cvt_move_;
  }

  int delay_interval() {
    return delay_interval_.load();
  }

  std::shared_ptr<std::string> hdfs_path_format_delay() {
    std::lock_guard<std::mutex> guard(mutex_);
    return hdfs_path_format_delay_;
  }

  std::unique_ptr<KafkaTopicConf> kafka_topic_conf() {
    return nullptr;
  }

  std::shared_ptr<std::string> rootdir() {
    return contents_.rootdir;
  }

  ConsumeCallback::Type consume_type() {
    return contents_.consume_type;
  }

  int64_t consume_offset() {
    return contents_.consume_offset;
  }

  int consume_interval() {
    return contents_.consume_interval.load();
  }

  long long consume_maxsize() {
    return contents_.consume_maxsize.load();
  }

  int consume_complete_interval() {
    return contents_.consume_complete_interval.load();
  }

  Compress::Type compress_type() {
    return contents_.compress_type;
  }
  
  int compress_interval() {
    return contents_.compress_interval.load();
  }

  std::shared_ptr<std::string> compress_lzo() {
    return contents_.compress_lzo;
  }

  Upload::Type upload_type() {
    return contents_.upload_type;
  }

  std::shared_ptr<std::string> upload_put() {
    return contents_.upload_put;
  }

  std::shared_ptr<std::string> upload_append() {
    return contents_.upload_append;
  }

  std::shared_ptr<std::string> upload_lzoindex() {
    return contents_.upload_lzoindex;
  }

 private:

  static K2HTopicConfContents DEFAULT_CONTENTS;

  std::mutex mutex_;
  std::shared_ptr<std::string> section_name_;
  std::shared_ptr<std::string> consume_dir_;
  std::shared_ptr<std::string> compress_dir_;
  std::shared_ptr<std::string> upload_dir_;
  std::shared_ptr<std::string> hdfs_path_format_;
  std::shared_ptr<std::string> cvt_move_;
  std::atomic<int> delay_interval_;
  std::shared_ptr<std::string> hdfs_path_format_delay_;
  K2HTopicConfContents contents_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_KAFKA2HDFS_TOPIC_CONF_H_
