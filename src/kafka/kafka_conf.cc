// Copyright (c) 2017 Lanceolata

#include "kafka/kafka_conf.h"

namespace log2hdfs {

// ------------------------------------------------------------------
// KafkaGlobalConf

std::unique_ptr<KafkaGlobalConf> KafkaGlobalConf::Init(
    KafkaGlobalConf::Type type) {
  return std::unique_ptr<KafkaGlobalConf>(new KafkaGlobalConf(type));
}

KafkaConfResult KafkaGlobalConf::Set(const std::string& name,
                                     const std::string& value,
                                     std::string* errstr) {
  if (name.empty() || value.empty()) {
    if (errstr)
      *errstr = "config empty";
    return KafkaConfResult::kConfEmpty;
  }

  rd_kafka_conf_res_t res;
  char errbuf[512];
  res = rd_kafka_conf_set(rk_conf_, name.c_str(), value.c_str(),
                          errbuf, sizeof(errbuf));
  if (res != RD_KAFKA_CONF_OK && errstr)
    *errstr = errbuf;

  return static_cast<KafkaConfResult>(res);
}

KafkaConfResult KafkaGlobalConf::Get(const std::string& name,
                                     std::string* value) const {
  if (name.empty() || !value)
    return KafkaConfResult::kConfEmpty;

  size_t size;
  rd_kafka_conf_res_t res = RD_KAFKA_CONF_OK;
  if ((res = rd_kafka_conf_get(rk_conf_, name.c_str(), NULL, &size))
          != RD_KAFKA_CONF_OK)
    return static_cast<KafkaConfResult>(res);

  std::unique_ptr<char[]> msg(new char[size]);
  if ((res = rd_kafka_conf_get(rk_conf_, name.c_str(), msg.get(), &size))
          != RD_KAFKA_CONF_OK)
    return static_cast<KafkaConfResult>(res);

  (*value).assign(msg.get());
  return KafkaConfResult::kConfOk;
}

// ------------------------------------------------------------------
// KafkaTopicConf

std::unique_ptr<KafkaTopicConf> KafkaTopicConf::Init(
    KafkaTopicConf::Type type) {
  return std::unique_ptr<KafkaTopicConf>(new KafkaTopicConf(type));
}

KafkaConfResult KafkaTopicConf::Set(const std::string& name,
                                    const std::string& value,
                                    std::string* errstr) {
  if (name.empty() || value.empty()) {
    if (errstr)
      *errstr = "config empty";
    return KafkaConfResult::kConfEmpty;
  }

  rd_kafka_conf_res_t res;
  char errbuf[512];
  res = rd_kafka_topic_conf_set(rkt_conf_, name.c_str(), value.c_str(),
                                errbuf, sizeof(errbuf));
  if (res != RD_KAFKA_CONF_OK && errstr)
    *errstr = errbuf;

  return static_cast<KafkaConfResult>(res);
}

KafkaConfResult KafkaTopicConf::Get(const std::string& name,
                                    std::string* value) const {
  if (name.empty() || !value)
    return KafkaConfResult::kConfEmpty;

  size_t size;
  rd_kafka_conf_res_t res = RD_KAFKA_CONF_OK;
  if ((res = rd_kafka_topic_conf_get(rkt_conf_, name.c_str(), NULL, &size))
          != RD_KAFKA_CONF_OK)
    return static_cast<KafkaConfResult>(res);

  std::unique_ptr<char[]> msg(new char[size]);
  if ((res = rd_kafka_topic_conf_get(rkt_conf_, name.c_str(), msg.get(), &size))
          != RD_KAFKA_CONF_OK)
    return static_cast<KafkaConfResult>(res);

  (*value).assign(msg.get());
  return KafkaConfResult::kConfOk;
}

}   // namespace log2hdfs
