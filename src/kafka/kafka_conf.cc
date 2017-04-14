// Copyright (c) 2017 Lanceolata

#include "kafka/kafka_conf.h"
#include "util/logger.h"

namespace log2hdfs {

// ------------------------------------------------------------------
// KafkaGlobalProduerConf


// ------------------------------------------------------------------
// KafkaGlobalConsumerConf

class KafkaGlobalConsumerConf : public KafkaGlobalConf {
 public:
  static std::unique_ptr<KafkaGlobalConf> Init() {
    return std::unique_ptr<KafkaGlobalConf>(new KafkaGlobalConsumerConf());
  }

  explicit KafkaGlobalConsumerConf(rd_kafka_conf_t *rk_conf = NULL):
      KafkaGlobalConf(rk_conf) {
    rd_kafka_conf_set_error_cb(
        rk_conf_, &(KafkaGlobalConsumerConf::error_cb));
  }

  ~KafkaGlobalConsumerConf() {}

  KafkaGlobalConsumerConf(const KafkaGlobalConsumerConf &other) = delete;
  KafkaGlobalConsumerConf &operator=(
      const KafkaGlobalConsumerConf &other) = delete;

  static void error_cb(rd_kafka_t *rk, int err,
                       const char *reason, void *opaque);
};

void KafkaGlobalConsumerConf::error_cb(rd_kafka_t *rk, int err,
                                       const char *reason,
                                       void *opaque) {
  Log(LogLevel::kLogError, "rdkafka error cb: %s: %s: %s",
      rd_kafka_name(rk), rd_kafka_err2str((rd_kafka_resp_err_t)err),
      reason);
}


// ------------------------------------------------------------------
// KafkaGlobalConf

std::unique_ptr<KafkaGlobalConf> KafkaGlobalConf::Init(
    KafkaGlobalConf::Type type) {
  std::unique_ptr<KafkaGlobalConf> res;
  switch (type) {
    case kProducer:
      res = std::unique_ptr<KafkaGlobalConf>(new KafkaGlobalConf());
      break;
    case kConsumer:
      res = KafkaGlobalConsumerConf::Init();
      break;
    default:
      res = nullptr;
  }
  return res;
}

KafkaConfResult KafkaGlobalConf::Set(const std::string &name,
                                     const std::string &value,
                                     std::string *errstr) {
  if (name.empty() || value.empty()) {
    *errstr = "config empty";
    return KafkaConfResult::kConfEmpty;
  }

  rd_kafka_conf_res_t res;
  char errbuf[512];
  res = rd_kafka_conf_set(rk_conf_, name.c_str(), value.c_str(),
                          errbuf, sizeof(errbuf));
  if (res != RD_KAFKA_CONF_OK && errstr != NULL) {
    *errstr = errbuf;
  }
  return static_cast<KafkaConfResult>(res);
}

KafkaConfResult KafkaGlobalConf::Get(const std::string &name,
                                     std::string *value) const {
  if (name.empty() || value == NULL) {
    return KafkaConfResult::kConfEmpty;
  }

  size_t size;
  rd_kafka_conf_res_t res = RD_KAFKA_CONF_OK;
  if ((res = rd_kafka_conf_get(rk_conf_, name.c_str(), NULL, &size))
          != RD_KAFKA_CONF_OK) {
    return static_cast<KafkaConfResult>(res);
  }

  std::unique_ptr<char[]> msg(new char[size]);
  if ((res = rd_kafka_conf_get(rk_conf_, name.c_str(), msg.get(), &size))
          != RD_KAFKA_CONF_OK) {
    return static_cast<KafkaConfResult>(res);
  }

  (*value).assign(msg.get());
  return KafkaConfResult::kConfOk;
}


// ------------------------------------------------------------------
// KafkaTopicProducerConf


// ------------------------------------------------------------------
// KafkaTopicConsumerConf


// ------------------------------------------------------------------
// KafkaTopicConf

std::unique_ptr<KafkaTopicConf> KafkaTopicConf::Init(
    KafkaTopicConf::Type type) {
  std::unique_ptr<KafkaTopicConf> res;
  switch (type) {
    case kProducer:
      res = std::unique_ptr<KafkaTopicConf>(new KafkaTopicConf());
      break;
    case kConsumer:
      res = std::unique_ptr<KafkaTopicConf>(new KafkaTopicConf());
      break;
    default:
      res = nullptr;
  }
  return res;
}

KafkaConfResult KafkaTopicConf::Set(const std::string &name,
                                    const std::string &value,
                                    std::string *errstr) {
  if (name.empty() || value.empty()) {
    *errstr = "config empty";
    return KafkaConfResult::kConfEmpty;
  }

  rd_kafka_conf_res_t res;
  char errbuf[512];
  res = rd_kafka_topic_conf_set(rkt_conf_, name.c_str(), value.c_str(),
                                errbuf, sizeof(errbuf));
  if (res != RD_KAFKA_CONF_OK && errstr != NULL) {
    *errstr = errbuf;
  }
  return static_cast<KafkaConfResult>(res);
}

KafkaConfResult KafkaTopicConf::Get(const std::string &name,
                                    std::string *value) const {
  if (name.empty() || value == NULL) {
    return KafkaConfResult::kConfEmpty;
  }

  size_t size;
  rd_kafka_conf_res_t res = RD_KAFKA_CONF_OK;
  if ((res = rd_kafka_topic_conf_get(rkt_conf_, name.c_str(), NULL, &size))
          != RD_KAFKA_CONF_OK) {
    return static_cast<KafkaConfResult>(res);
  }

  std::unique_ptr<char[]> msg(new char[size]);
  if ((res = rd_kafka_topic_conf_get(rkt_conf_, name.c_str(), msg.get(), &size))
          != RD_KAFKA_CONF_OK) {
    return static_cast<KafkaConfResult>(res);
  }

  (*value).assign(msg.get());
  return KafkaConfResult::kConfOk;
}

}   // namespace log2hdfs
