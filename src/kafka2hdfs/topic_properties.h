// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_TOPIC_CONF_H_
#define LOG2HDFS_KAFKA2HDFS_TOPIC_CONF_H_

#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include "util/configparser.h"
#include "kafka2hdfs/compress.h"
#include "kafka/kafka_conf.h"

namespace log2hdfs {

namespace kafka2hdfs {

class PathFormat;

class TopicConf {
 public:
  std::unique_ptr<TopicConf> Init(std::shared_ptr<util::Section> section);

  std::string topic() {
    return topic_;
  }

  bool set_topic(const std::string &topic) {
    if (topic.empty())
      return false;    

    topic_ = topic;
    return true;
  }

  std::vector<int> partitions() {
    return partitions_;
  }

  bool set_partitions(const std::string &partitions) {
    if (partitions.empty())
      return false;
  }

 private:
  std::string topic_;
  std::vector<int> partitions_;
  std::unordered_map<std::string, std::string> kafka_confs_;
  std::string local_dir_;
  Compress::Type compress_;
  std::string compress_dir_;
  std::string upload_dir_;
  int64_t maxsize_;
  int interval;
  int compress_interval;
  int enqueue_interval;
  int upload_interval;
  std::shared_ptr<PathFormat> path_format_;
};

}   // namespace kafka2hdfs

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_TOPIC_CONF_H_
