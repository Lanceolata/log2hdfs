// Copyright (c) 2017 Lanceolata

#include "kafka/kafka_consumer.h"
#include "kafka/kafka_conf.h"
#include "kafka/kafka_topic.h"
#include "kafka/kafka_topic_consumer.h"
#include "kafka/kafka_error.h"
#include "easylogging++.h"

namespace log2hdfs {

std::shared_ptr<KafkaConsumer> KafkaConsumer::Init(
    KafkaGlobalConf* conf, std::string* errstr) {
  if (!conf) {
    if (errstr)
      *errstr = "Invalid parameters";
    return nullptr;
  }

  char errbuf[512];
  rd_kafka_conf_t *rk_conf = rd_kafka_conf_dup(conf->rk_conf_);
  rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_CONSUMER, rk_conf,
                                errbuf, sizeof(errbuf));
  if (!rk) {
    if (errstr)
      *errstr = std::string(errbuf);
    rd_kafka_conf_destroy(rk_conf);
    return nullptr;
  }

  std::shared_ptr<KafkaHandle> handle = KafkaHandle::Init(rk);
  if (!handle) {
    if (errstr)
      *errstr = "KafkaHandle init failed";
    return nullptr;
  }
  return std::make_shared<KafkaConsumer>(std::move(handle));
}

std::shared_ptr<KafkaTopicConsumer> KafkaConsumer::CreateTopicConsumer(
    const std::string& topic,
    KafkaTopicConf* conf,
    const std::vector<int32_t>& partitions,
    const std::vector<int64_t>& offsets,
    std::shared_ptr<KafkaConsumeCb> cb,
    std::string* errstr) {
  if (topic.empty() || !conf || partitions.empty() ||
          offsets.empty() || !cb) {
    if (errstr)
      *errstr = "Invlaid parameters";
    return nullptr;
  }

  std::lock_guard<std::mutex> guard(mutex_);
  auto it = topics_.find(topic);
  if (it != topics_.end()) {
    if (errstr)
      *errstr = "topic consumer already created";
    return nullptr;
  }

  rd_kafka_topic_conf_t *rkt_conf = rd_kafka_topic_conf_dup(conf->rkt_conf_);
  rd_kafka_topic_t *rkt = rd_kafka_topic_new(handle_->rk_, topic.c_str(),
                                             rkt_conf);
  if (!rkt) {
    if (errstr)
      *errstr = KafkaErrnoToStr(errno);
    rd_kafka_topic_conf_destroy(rkt_conf);
    return nullptr;
  }

  std::shared_ptr<KafkaTopic> kt = KafkaTopic::Init(rkt);
  if (!kt) {
    if (errstr)
      *errstr = "KafkaTopic init failed";
    return nullptr;
  }

  std::shared_ptr<KafkaTopicConsumer> ktc;
  ktc = KafkaTopicConsumer::Init(handle_, kt, partitions,
                                 offsets, std::move(cb));
  if (!ktc) {
    if (errstr)
      *errstr = "KafkaTopicConsumer init failed";
    return nullptr;
  }
  topics_.insert(std::make_pair(topic, ktc));
  return ktc;
}

void KafkaConsumer::StartAllTopic() {
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto it = topics_.begin(); it != topics_.end(); ++it) {
    it->second->Start();
  }
}

void KafkaConsumer::StopAllTopic() {
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto it = topics_.begin(); it != topics_.end(); ++it) {
    it->second->Stop();
  }
  for (auto it = topics_.begin(); it != topics_.end(); ++it) {
    it->second->Join();
  }
}

bool KafkaConsumer::StartTopic(const std::string& topic) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = topics_.find(topic);
  if (it == topics_.end())
    return false;

  it->second->Start();
  return true;
}

bool KafkaConsumer::StopTopic(const std::string& topic) {
  std::unique_lock<std::mutex> lock(mutex_);
  auto it = topics_.find(topic);
  if (it == topics_.end())
    return false;

  std::shared_ptr<KafkaTopicConsumer> ktc = it->second;
  topics_.erase(it);
  lock.unlock();

  it->second->Stop();
  it->second->Join();
  return true;
}

}   // namespace log2hdfs
