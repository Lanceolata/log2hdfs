// Copyright (c) 2017 Lanceolata

#include "kafka/kafka_consumer.h"
#include <utility>
#include "kafka/kafka_conf.h"
#include "kafka/kafka_topic.h"
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

KafkaConsumer::~KafkaConsumer() {
  std::lock_guard<std::mutex> guard(mutex_);
  auto it = topics_.begin();
  for (; it != topics_.end(); ++it) {
    it->second->StopAllPartitionConsumer();
  }
  handle_->PollOutq(0, 5000);
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

  std::shared_ptr<KafkaTopic> kt = KafkaTopic::Init(rkt, handle_);
  if (!kt) {
    Log(LogLevel::kLogError, "CreateTopic topic[%s] failed",
        topic.c_str());
  } else {
    Log(LogLevel::kLogInfo, "CreateTopic topic[%s] success",
        topic.c_str());
    topics_.insert(std::make_pair(topic, kt));
  }
  return kt;
}

}   // namespace log2hdfs
