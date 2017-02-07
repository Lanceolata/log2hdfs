// Copyright (c) 2017 Lanceolata

#ifndef LOG2KAFKA_KAFKA_CONFIMP_H_
#define LOG2KAFKA_KAFKA_CONFIMP_H_

#include "conf.h"

namespace log2hdfs {

namespace kafka {

// ------------------------------------------------------------------
// GlobalConfImp

class GlobalConfImp : public GlobalConf {
 public:
  explicit GlobalConfImp(rd_kafka_conf_t *rk_conf): rk_conf_(rk_conf) {}

  GlobalConfImp(const GlobalConfImp &gci) = delete;
  GlobalConfImp &operator=(const GlobalConfImp &gci) = delete;

  ~GlobalConfImp() {}

  ConfResult Set(const std::string &name, const std::string &value,
                 std::string *errstr);

  ConfResult Get(const std::string &name, std::string *value) const;

  rd_kafka_conf_t *conf() {
    return rk_conf_;
  }

 protected:
  rd_kafka_conf_t *rk_conf_;
};

// ------------------------------------------------------------------
// GlobalProducerConf



// ------------------------------------------------------------------
// GlobalConsumerConf

class GlobalConsumerConf : public GlobalConfImp {
 public:
  static std::unique_ptr<GlobalConf> Init(rd_kafka_conf_t *rk_conf = NULL) {
    if (rk_conf == NULL) {
      return std::unique_ptr<GlobalConf>(new GlobalConsumerConf());
    } else {
      return std::unique_ptr<GlobalConf>(new GlobalConsumerConf(rk_conf));
    }
  }

  GlobalConsumerConf(const GlobalConsumerConf &gcc) = delete;
  GlobalConsumerConf &operator=(const GlobalConsumerConf &gcc) = delete;

  ~GlobalConsumerConf() {}

  std::unique_ptr<GlobalConf> Copy() const {
    return std::unique_ptr<GlobalConf>(
            new GlobalConsumerConf(rd_kafka_conf_dup(rk_conf_)));
  }

  static void error_cb(rd_kafka_t *rk, int err, const char *reason,
                       void *opaque);

 private:
  explicit GlobalConsumerConf(rd_kafka_conf_t *rk_conf):
      GlobalConfImp(rk_conf) {
    rd_kafka_conf_set_error_cb(rk_conf_, &(GlobalConsumerConf::error_cb));
  }

  GlobalConsumerConf(): GlobalConsumerConf(rd_kafka_conf_new()) {}
};

}   // namespace kafka

}   // namespace log2hdfs

#endif  // LOG2KAFKA_KAFKA_CONFIMP_H_
