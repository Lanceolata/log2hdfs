// Copyright (c) 2017 Lanceolata

#include "kafka/kafka_conf.h"
#include "kafka/kafka_error.h"
#include "util/logger.h"

namespace log2hdfs {

// ------------------------------------------------------------------
// GlobalProducerConf
// ------------------------------------------------------------------
// GlobalConsumerConf

class GlobalConsumerConf : public GlobalConf {
 public:
  static std::unique_ptr<GlobalConf> Init() {
    return std::unique_ptr<GlobalConf>(new GlobalConsumerConf());
  }

  GlobalConsumerConf(const GlobalConsumerConf &gcc) = delete;
  GlobalConsumerConf &operator=(const GlobalConsumerConf &gcc) = delete;

  ~GlobalConsumerConf() {}

  static void error_cb(rd_kafka_t *rk, int err, const char *reason,
                       void *opaque);

 private:
  explicit GlobalConsumerConf(rd_kafka_conf_t *rk_conf = NULL):
      GlobalConf(rk_conf) {
    rd_kafka_conf_set_error_cb(rk_conf_, &(GlobalConsumerConf::error_cb));
  }
};

void GlobalConsumerConf::error_cb(rd_kafka_t *rk, int err,
                                  const char *reason,
                                  void *opaque) {
  Log(LogLevel::kLogError, "rdkafka error cb: %s: %s: %s",
      rd_kafka_name(rk),
      rd_kafka_err2str((rd_kafka_resp_err_t)err),
      reason);
}

// ------------------------------------------------------------------
// GlobalConf

std::unique_ptr<GlobalConf> GlobalConf::Init(GlobalConf::ConfType type) {
  std::unique_ptr<GlobalConf> res;

  switch (type) {
    case kConfProducer:
      res = nullptr;
      break;
    case kConfConsumer:
      res = GlobalConsumerConf::Init();
      break;
    default:
      res = nullptr;
  }

  return res;
}


ConfResult GlobalConf::Set(const std::string &name, const std::string &value,
                           std::string *errstr) {
  if (name.empty() || value.empty()) {
    *errstr = "config empty";
    return ConfResult::kConfEmpty;
  }

  rd_kafka_conf_res_t res;
  char errbuf[512];

  res = rd_kafka_conf_set(rk_conf_, name.c_str(), value.c_str(),
                          errbuf, sizeof(errbuf));

  if (res != RD_KAFKA_CONF_OK && errstr != NULL) {
    *errstr = errbuf;
  }

  return static_cast<ConfResult>(res);
}

ConfResult GlobalConf::Get(const std::string &name,
                           std::string *value) const {
  if (name.empty() || value == NULL) {
    return ConfResult::kConfEmpty;
  }

  size_t size;
  rd_kafka_conf_res_t res = RD_KAFKA_CONF_OK;

  if ((res = rd_kafka_conf_get(rk_conf_, name.c_str(), NULL, &size))
          != RD_KAFKA_CONF_OK) {
    return static_cast<ConfResult>(res);
  }

  std::unique_ptr<char[]> msg(new char[size]);
  if ((res = rd_kafka_conf_get(rk_conf_, name.c_str(), msg.get(), &size))
          != RD_KAFKA_CONF_OK) {
    return static_cast<ConfResult>(res);
  }

  (*value).assign(msg.get());
  return ConfResult::kConfOk;
}

// ------------------------------------------------------------------
// TopicProducerConf
// ------------------------------------------------------------------
// TopicConsumerConf

class TopicConsumerConf : public TopicConf {
 public:
  static std::unique_ptr<TopicConf> Init() {
    return std::unique_ptr<TopicConf>(new TopicConsumerConf());
  }

  TopicConsumerConf(const TopicConsumerConf &tcc) = delete;
  TopicConsumerConf &operator=(const TopicConsumerConf &tcc) = delete;

  ~TopicConsumerConf() {}

 private:
  explicit TopicConsumerConf(rd_kafka_topic_conf_t *rkt_conf = NULL):
      TopicConf(rkt_conf) {}
};

// ------------------------------------------------------------------
// TopicConf

std::unique_ptr<TopicConf> TopicConf::Init(TopicConf::ConfType type) {
    std::unique_ptr<TopicConf> res;

  switch (type) {
    case kConfProducer:
      res = TopicConsumerConf::Init();
      break;
    case kConfConsumer:
      res = TopicConsumerConf::Init();
      break;
    default:
      res = nullptr;
  }

  return res;
}

ConfResult TopicConf::Set(const std::string &name, const std::string &value,
                          std::string *errstr) {
  if (name.empty() || value.empty()) {
    *errstr = "config empty";
    return ConfResult::kConfEmpty;
  }

  rd_kafka_conf_res_t res;
  char errbuf[512];

  res = rd_kafka_topic_conf_set(rkt_conf_, name.c_str(), value.c_str(),
                                errbuf, sizeof(errbuf));

  if (res != RD_KAFKA_CONF_OK && errstr != NULL) {
    *errstr = errbuf;
  }

  return static_cast<ConfResult>(res);
}

ConfResult TopicConf::Get(const std::string &name,
                          std::string *value) const {
  if (name.empty() || value == NULL) {
    return ConfResult::kConfEmpty;
  }

  size_t size;
  rd_kafka_conf_res_t res = RD_KAFKA_CONF_OK;

  if ((res = rd_kafka_topic_conf_get(rkt_conf_, name.c_str(), NULL, &size))
          != RD_KAFKA_CONF_OK) {
    return static_cast<ConfResult>(res);
  }

  std::unique_ptr<char[]> msg(new char[size]);
  if ((res = rd_kafka_topic_conf_get(rkt_conf_, name.c_str(), msg.get(), &size))
          != RD_KAFKA_CONF_OK) {
    return static_cast<ConfResult>(res);
  }

  (*value).assign(msg.get());
  return ConfResult::kConfOk;
}

}   // namespace log2hdfs
