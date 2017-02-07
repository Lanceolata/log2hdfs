// Copyright (c) 2017 Lanceolata

#include "confimp.h"
#include "logger.h"

namespace log2hdfs {

namespace kafka {

// ------------------------------------------------------------------
// GlobalConf

std::unique_ptr<GlobalConf> GlobalConf::Create(GlobalConf::ConfType type) {
  std::unique_ptr<GlobalConf> res;

  switch (type) {
    case kConfProducer:
      res = nullptr;
      break;
    case kConfConsumer:
      res = GlobalConsumerConf::Init();
      break;
  }

  return res;
}

// ------------------------------------------------------------------
// GlobalConfImp

ConfResult GlobalConfImp::Set(const std::string &name,
                              const std::string &value,
                              std::string *errstr) {
  if (name.empty() || value.empty()) {
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

ConfResult GlobalConfImp::Get(const std::string &name,
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
// GlobalProducerConf


// ------------------------------------------------------------------
// GlobalConsumerConf

void GlobalConsumerConf::error_cb(rd_kafka_t *rk, int err, const char *reason,
                                  void *opaque) {
  Log(util::Logger::Level::kLogError, "rdkafka error cb: %s: %s: %s",
      rd_kafka_name(rk), rd_kafka_err2str((rd_kafka_resp_err_t)err), reason);
}

}   // namespace kafka

}   // namespace log2hdfs
