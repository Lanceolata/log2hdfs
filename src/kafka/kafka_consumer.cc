// Copyright (c) 2017 Lanceolata

#include "kafka/kafka_consumer.h"
#include <thread>
#include <utility>
#include "kafka/kafka_conf.h"
#include "kafka/kafka_handle.h"
#include "kafka/kafka_topic.h"
#include "kafka/kafka_partition_consumer.h"
#include "kafka/kafka_error.h"
#include "util/logger.h"

namespace log2hdfs {

std::shared_ptr<KafkaConsumer> KafkaConsumer::Init(KafkaGlobalConf *conf) {
  if (!conf || !conf->rk_conf_) {
    Log(LogLevel::kLogError, "KafkaConsumer init failed with invalid conf");
    return nullptr;
  }

  char errbuf[512];
  rd_kafka_conf_t *rk_conf = rd_kafka_conf_dup(conf->rk_conf_);
  rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_CONSUMER, rk_conf,
                                errbuf, sizeof(errbuf));
  if (!rk) {
    Log(LogLevel::kLogError, "KafkaConsumer init failed with error[%s]",
        errbuf);
    rd_kafka_conf_destroy(rk_conf);
    return nullptr;
  }

  return std::make_shared<KafkaConsumer>(rk);
}

std::shared_ptr<KafkaTopic> KafkaConsumer::CreateTopic(
    const std::string &topic, KafkaTopicConf *conf) {
  if (topic.empty() || !conf || !conf->rkt_conf_) {
    Log(LogLevel::kLogError, "CreateTopic topic[%s] failed with invalid conf",
        topic.c_str());
    return nullptr;
  }

  std::lock_guard<std::mutex> guard(mutex_);

  auto it = topics_.find(topic);
  if (it != topics_.end()) {
    return it->second;
  }

  rd_kafka_topic_conf_t *rkt_conf = rd_kafka_topic_conf_dup(conf->rkt_conf_);
  rd_kafka_topic_t *rkt = rd_kafka_topic_new(handle_->rk_, topic.c_str(),
                                             rkt_conf);
  if (!rkt) {
    Log(LogLevel::kLogError, "KafkaConsumer init failed with error[%s]",
        KafkaErrnoToStr(errno).c_str());
    rd_kafka_topic_conf_destroy(rkt_conf);
    return nullptr;
  }

  std::shared_ptr<KafkaTopic> kt = KafkaTopic::Init(rkt);
  if (kt) {
    Log(LogLevel::kLogInfo, "CreateTopic success topic[%s]",
        topic.c_str());
    topics_.insert(std::make_pair(topic, kt));
  }
  return kt;
}

std::shared_ptr<KafkaPartitionConsumer> KafkaConsumer::CreatePartitionConsumer(
    const std::string &topic, int32_t partition,
    int64_t offset, std::shared_ptr<KafkaPartitionConsumerCb> cb) {
  std::lock_guard<std::mutex> guard(mutex_);

  auto it = topics_.find(topic);
  if (it == topics_.end()) {
    Log(LogLevel::kLogError, "CreatePartitionConsumer failed invalid "
        "topic[%s]", topic.c_str());
    return nullptr;
  }

  std::shared_ptr<KafkaTopic> kt = it->second;
  std::shared_ptr<KafkaPartitionConsumer> kpc;
  kpc = KafkaPartitionConsumer::Init(handle_, kt, partition,
                                     offset, cb);
  if (kpc) {
    Log(LogLevel::kLogInfo, "CreatePartitionConsumer success topic[%s] "
        "partition[%d] offset[%ld]", topic.c_str(), partition, offset);
    consumers_.insert(std::make_pair(topic, kpc));
  }
  return kpc;
}

bool KafkaConsumer::StartTopic(const std::string &topic) {
  std::lock_guard<std::mutex> guard(mutex_);
  auto range = consumers_.equal_range(topic);
  if (range.first == range.second) {
    Log(LogLevel::kLogError, "StartTopic failed invalid topic[%s]",
        topic.c_str());
    return false;
  }
  for (auto it = range.first; it != range.second; ++it) {
    std::shared_ptr<KafkaPartitionConsumer> kpc = it->second;
    if (!kpc->running()) {
      std::thread t(&KafkaPartitionConsumer::Start, kpc.get());
      t.detach();
    }
  }
  return true;
}

void KafkaConsumer::StopAll(int milli) {
  std::lock_guard<std::mutex> guard(mutex_);
  for (auto it = consumers_.begin(); it != consumers_.end(); ++it) {
    it->second->Stop();
  }
  while (handle_->OutqLen() > 0) {
    handle_->Poll(milli);
  }
}

}   // namespace log2hdfs
