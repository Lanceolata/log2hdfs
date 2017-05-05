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

class KafkaProducer;
class KafkaConsumer;

// KafkaConf Set() result code
enum KafkaConfResult {
  kConfEmpty = -3,      // Empty configuration property
  kConfUnknown = -2,    // Unknown configuration property
  kConfInvalid = -1,    // Invalid configuration value
  kConfOk = 0           // Configuration property was succesfully set
};

// ------------------------------------------------------------------
// KafkaGlobalConf

class KafkaGlobalConf {
 public:
  // Kafka Global configuration object type
  enum class Type {
    kProducer,    // Global producer default configuration
    kConsumer    // Global consumer default configuration
  };

  static std::unique_ptr<KafkaGlobalConf> Init(KafkaGlobalConf::Type type);

  KafkaGlobalConf(KafkaGlobalConf::Type type,
                  rd_kafka_conf_t* rk_conf = NULL):
      type_(type), rk_conf_(rk_conf) {
    if (!rk_conf_)
      rk_conf_ = rd_kafka_conf_new();
  }

  ~KafkaGlobalConf() {
    if (rk_conf_)
      rd_kafka_conf_destroy(rk_conf_);
  }

  KafkaGlobalConf(const KafkaGlobalConf& other) = delete;
  KafkaGlobalConf& operator=(const KafkaGlobalConf& other) = delete;

  // Set conf name <--> value
  // param:
  //  - name            name of configuration
  //  - value           value of configuration
  //  - errstr          std::string pointer, if Set failed, reason will
  //                    write in *errstr
  //
  // return: KafkaConfResult
  KafkaConfResult Set(const std::string& name, const std::string& value,
                      std::string* errstr);

  // Get conf value with name
  KafkaConfResult Get(const std::string& name, std::string* value) const;

  // Set err callback
  bool SetErrorCb(void (*err_cb)(rd_kafka_t* rk, int err,
                                 const char* reason,
                                 void* opaque)) {
    if (err_cb == NULL)
      return false;

    rd_kafka_conf_set_error_cb(rk_conf_, err_cb);
    return true;
  }

  // Set deliver message callback
  bool SetDrMsgCb(void (*dr_msg_cb)(rd_kafka_t* rk,
                                    const rd_kafka_message_t* rkmessage,
                                    void* opaque)) {
    if (type_ != KafkaGlobalConf::Type::kProducer || dr_msg_cb == NULL)
      return false;

    rd_kafka_conf_set_dr_msg_cb(rk_conf_, dr_msg_cb);
    return true;
  }

 private:
  friend class KafkaProducer;
  friend class KafkaConsumer;

  KafkaGlobalConf::Type type_;
  rd_kafka_conf_t* rk_conf_;
};

// ------------------------------------------------------------------
// KafkaTopicConf

class KafkaTopicConf {
 public:
  // Kafka topic configuration object type
  enum class Type {
    kProducer,  // Topic producer default configuration
    kConsumer   // Topic consumer default configuration
  };

  static std::unique_ptr<KafkaTopicConf> Init(KafkaTopicConf::Type type);

  KafkaTopicConf(KafkaTopicConf::Type type,
                 rd_kafka_topic_conf_t* rkt_conf = NULL):
      type_(type), rkt_conf_(rkt_conf) {
    if (!rkt_conf_)
      rkt_conf_ = rd_kafka_topic_conf_new();
  }

  ~KafkaTopicConf() {
    if (rkt_conf_)
      rd_kafka_topic_conf_destroy(rkt_conf_);
  }

  KafkaTopicConf(const KafkaTopicConf& other) = delete;
  KafkaTopicConf& operator=(const KafkaTopicConf& other) = delete;

  // Set conf name <--> value
  // param:
  //  - name            name of configuration
  //  - value           value of configuration
  //  - errstr          std::string pointer, if Set failed, reason will
  //                    write in *errstr
  //
  // return: KafkaConfResult
  KafkaConfResult Set(const std::string& name, const std::string& value,
                      std::string* errstr);

  // Get conf value with name
  KafkaConfResult Get(const std::string& name, std::string* value) const;

  std::unique_ptr<KafkaTopicConf> Copy() const {
    return std::unique_ptr<KafkaTopicConf>(
               new KafkaTopicConf(type_, rd_kafka_topic_conf_dup(rkt_conf_)));
  }

 private:
  friend class KafkaProducer;
  friend class KafkaConsumer;

  KafkaTopicConf::Type type_;
  rd_kafka_topic_conf_t* rkt_conf_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAFKA_CONF_H_
