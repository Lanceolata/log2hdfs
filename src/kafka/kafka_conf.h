// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA_KAFKA_CONF_H_
#define LOG2HDFS_KAFKA_KAFKA_CONF_H_

#include <string>
#include <memory>

#ifdef __cplusplus
extern "C" {
#endif
#include "librdkafka/rdkafka.h"
#ifdef __cplusplus
}
#endif

namespace log2hdfs {

// Conf Set() result code
enum ConfResult {
  kConfEmpty = -3,      // Empty configuration property
  kConfUnknown = -2,    // Unknown configuration property
  kConfInvalid = -1,    // Invalid configuration value
  kConfOk = 0           // Configuration property was succesfully set
};

class Producer;
class Consumer;

// ------------------------------------------------------------------
// GlobalConf

class GlobalConf {
 public:
  // Configuration object type
  enum ConfType {
    kConfProducer,    // Global producer default configuration
    kConfConsumer    // Global consumer default configuration
  };

  static std::unique_ptr<GlobalConf> Init(GlobalConf::ConfType type);

  GlobalConf(const GlobalConf &gc) = delete;
  GlobalConf &operator=(const GlobalConf &gc) = delete;

  ~GlobalConf() {
    if (rk_conf_) {
      rd_kafka_conf_destroy(rk_conf_);
    }
  }

  ConfResult Set(const std::string &name, const std::string &value,
                 std::string *errstr);

  ConfResult Get(const std::string &name, std::string *value) const;

  std::unique_ptr<GlobalConf> Copy() const {
    return std::unique_ptr<GlobalConf>(
            new GlobalConf(rd_kafka_conf_dup(rk_conf_)));
  }

 protected:
  friend class Producer;
  friend class Consumer;

  explicit GlobalConf(rd_kafka_conf_t *rk_conf): rk_conf_(rk_conf) {
    if (!rk_conf_) {
      rk_conf_ = rd_kafka_conf_new();
    }
  }

  rd_kafka_conf_t *rk_conf_;
};


// ------------------------------------------------------------------
// TopicConf

class TopicConf {
 public:
  // Configuration object type
  enum ConfType {
    kConfProducer,  // Topic producer default configuration
    kConfConsumer   // Topic consumer default configuration
  };

  static std::unique_ptr<TopicConf> Init(TopicConf::ConfType type);

  TopicConf(const TopicConf &tc) = delete;
  TopicConf &operator=(const TopicConf &tc) = delete;

  ~TopicConf() {
    if (rkt_conf_) {
      rd_kafka_topic_conf_destroy(rkt_conf_);
    }
  }

  ConfResult Set(const std::string &name, const std::string &value,
                 std::string *errstr);

  ConfResult Get(const std::string &name, std::string *value) const;

  std::unique_ptr<TopicConf> Copy() const {
    return std::unique_ptr<TopicConf>(
            new TopicConf(rd_kafka_topic_conf_dup(rkt_conf_)));
  }

 protected:
  friend class Producer;
  friend class Consumer;

  explicit TopicConf(rd_kafka_topic_conf_t *rkt_conf): rkt_conf_(rkt_conf) {
    if (!rkt_conf_) {
      rkt_conf_ = rd_kafka_topic_conf_new();
    }
  }

  rd_kafka_topic_conf_t *rkt_conf_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAFKA_CONF_H_
