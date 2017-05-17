// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_TOPIC_CONF_H_
#define LOG2HDFS_KAFKA2HDFS_TOPIC_CONF_H_

#include <string>
#include <vector>
#include <memory>
#include <mutex>
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
};

class TopicConf {
 public:
  // Must call before new TopicConf.
  static bool UpdataDefaultConf(std::shared_ptr<Section> section);

  static std::shared_ptr<TopicConf> Init(const std::string& section,
                                         const std::string& rootdir);

  TopicConf(const std::string& section, const std::string& rootdir):
      section_(section), contents_(DEFAULT_CONTENTS_) {
    consume_dir_ = rootdir + "/consume";
    compress_dir_ = rootdir + "/compress";
    upload_dir_ = rootdir + "/upload";
  }

  bool InitConf(std::shared_ptr<Section> section);

  // This function runtime update conf safe.
  bool UpdateRuntime(std::shared_ptr<Section> section);

  std::string consume_dir() const {
    return consume_dir_;
  }

  std::string compress_dir() const {
    return compress_dir_;
  }

  std::string upload_dir() const {
    return upload_dir_;
  }

 private:
  static TopicConfContents DEFAULT_CONTENTS_;

  std::string section_;
  std::string consume_dir_;
  std::string compress_dir_;
  std::string upload_dir_;
  std::vector<std::string> topics_;
  std::vector<std::vector<int32_t>> partitions_;
  std::vector<std::vector<int64_t>> offsets_;
  std::string hdfs_path_;
  TopicConfContents contents_;
  std::mutex mutex_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_TOPIC_CONF_H_
