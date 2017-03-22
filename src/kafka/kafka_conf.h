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

#include "util/optional.h"

namespace log2hdfs {

// Conf Set() result code
enum ConfResult {
  kConfEmpty = -3,      // Empty configuration property
  kConfUnknown = -2,    // Unknown configuration property
  kConfInvalid = -1,    // Invalid configuration value
  kConfOk = 0           // Configuration property was succesfully set
};

class KafkaProducer;
class KafkaConsumer;

// ------------------------------------------------------------------
// KafkaGlobalConf

class KafkaGlobalConf {
 public:
  // Configuration object type
  enum Type {
    kProducer,    // Global producer default configuration
    kConsumer    // Global consumer default configuration
  };

  static Optional<KafkaGlobalConf::Type> GetTypeFromString(
      const std::string &type);

  static std::unique_ptr<KafkaGlobalConf> Init(KafkaGlobalConf::Type type);

  explicit KafkaGlobalConf(rd_kafka_conf_t *rk_conf = NULL):
      rk_conf_(rk_conf) {
    if (!rk_conf_) {
      rk_conf_ = rd_kafka_conf_new();
    }
  }

  KafkaGlobalConf(const KafkaGlobalConf &other) = delete;
  KafkaGlobalConf &operator=(const KafkaGlobalConf &other) = delete;

  ~KafkaGlobalConf() {
    if (rk_conf_) {
      rd_kafka_conf_destroy(rk_conf_);
    }
  }

  ConfResult Set(const std::string &name, const std::string &value,
                 std::string *errstr);

  ConfResult Get(const std::string &name, std::string *value) const;

  std::unique_ptr<KafkaGlobalConf> Copy() const {
    return std::unique_ptr<KafkaGlobalConf>(
               new KafkaGlobalConf(rd_kafka_conf_dup(rk_conf_)));
  }

 protected:
  friend class KafkaProducer;
  friend class KafkaConsumer;

  rd_kafka_conf_t *rk_conf_;
};

// ------------------------------------------------------------------
// KafkaTopicConf

class KafkaTopicConf {
 public:
  // Configuration object type
  enum Type {
    kProducer,  // Topic producer default configuration
    kConsumer   // Topic consumer default configuration
  };

  static Optional<KafkaTopicConf::Type> GetTypeFromString(
      const std::string &type);

  static std::unique_ptr<KafkaTopicConf> Init(KafkaTopicConf::Type type);

  explicit KafkaTopicConf(rd_kafka_topic_conf_t *rkt_conf = NULL):
      rkt_conf_(rkt_conf) {
    if (!rkt_conf_) {
      rkt_conf_ = rd_kafka_topic_conf_new();
    }
  }

  KafkaTopicConf(const KafkaTopicConf &other) = delete;
  KafkaTopicConf &operator=(const KafkaTopicConf &other) = delete;

  ~KafkaTopicConf() {
    if (rkt_conf_) {
      rd_kafka_topic_conf_destroy(rkt_conf_);
    }
  }

  ConfResult Set(const std::string &name, const std::string &value,
                 std::string *errstr);

  ConfResult Get(const std::string &name, std::string *value) const;

  std::unique_ptr<KafkaTopicConf> Copy() const {
    return std::unique_ptr<KafkaTopicConf>(
               new KafkaTopicConf(rd_kafka_topic_conf_dup(rkt_conf_)));
  }

 protected:
  friend class KafkaProducer;
  friend class KafkaConsumer;

  rd_kafka_topic_conf_t *rkt_conf_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAFKA_CONF_H_
