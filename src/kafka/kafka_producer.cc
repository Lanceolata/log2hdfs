// Copyright (c) 2017 Lanceolata

#include "kafka/kafka_producer.h"
#include "kafka/kafka_conf.h"
#include "kafka/kafka_topic.h"
#include "kafka/kafka_topic_producer.h"
#include "kafka/kafka_error.h"

namespace log2hdfs {

std::shared_ptr<KafkaProducer> KafkaProducer::Init(
    KafkaGlobalConf* conf, std::string *errstr) {
  if (!conf) {
    if (errstr)
      *errstr = "Invalid conf";
    return nullptr;
  }

  char errbuf[512];
  rd_kafka_conf_t *rk_conf = rd_kafka_conf_dup(conf->rk_conf_);
  rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_PRODUCER, rk_conf,
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
  return std::make_shared<KafkaProducer>(std::move(handle));
}

std::shared_ptr<KafkaTopicProducer> KafkaProducer::CreateTopicProducer(
    const std::string& topic, KafkaTopicConf* conf, std::string* errstr) {
  if (topic.empty() || !conf) {
    if (errstr)
      *errstr = "Invalid parameters topic[" + topic + "]";
    return nullptr;
  }

  std::lock_guard<std::mutex> guard(mutex_);
  auto it = topics_.find(topic);
  if (it != topics_.end())
    return it->second;

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

  std::shared_ptr<KafkaTopicProducer> ktp;
  ktp = KafkaTopicProducer::Init(handle_, kt);
  if (!ktp) {
    if (errstr)
      *errstr = "KafkaTopicProducer Init failed";
    return nullptr;
  }
  topics_.insert(std::make_pair(topic, ktp));
  return ktp;
}

std::shared_ptr<KafkaTopicProducer> KafkaProducer::GetTopicProducer(
    const std::string& topic) {
  if (topic.empty())
    return nullptr;

  std::lock_guard<std::mutex> guard(mutex_);
  auto it = topics_.find(topic);
  if (it != topics_.end()) {
    return it->second;
  } else {
    return nullptr;
  }
}

bool KafkaProducer::RemoveTopicProducer(const std::string& topic) {
  if (topic.empty())
    return false;

  std::lock_guard<std::mutex> guard(mutex_);
  return topics_.erase(topic);
}

}   // namespace log2hdfs
