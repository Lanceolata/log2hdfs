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

/**
 * KafkaConf Set() result code
 */
enum KafkaConfResult {
  kConfEmpty = -3,      /**< Empty configuration property. */
  kConfUnknown = -2,    /**< Unknown configuration property. */
  kConfInvalid = -1,    /**< Invalid configuration value. */
  kConfOk = 0           /**< Configuration property was succesfully set. */
};

// ------------------------------------------------------------------
// KafkaGlobalConf

/**
 * Global Configuration interface.
 * 
 * Holds global configuration that are passed to KafkaProducer,
 * KafkaConsumer.
 */
class KafkaGlobalConf {
 public:
  /**
   * Static function to create a KafkaGlobalConf unique_ptr.
   * 
   * @return std::unique_ptr<KafkaGlobalConf>.
   */
  static std::unique_ptr<KafkaGlobalConf> Init();

  /**
   * Constructor.
   * 
   * @param type        configuration type(producer or consumer).
   * @param rk_conf     librdkafka global configuration raw pointer.
   */
  KafkaGlobalConf(rd_kafka_conf_t* rk_conf = NULL):
      rk_conf_(rk_conf) {
    if (!rk_conf_)
      rk_conf_ = rd_kafka_conf_new();
  }

  /**
   * Destructor.
   * 
   * If rk_conf_ valid, destroy rk_conf_.
   */
  ~KafkaGlobalConf() {
    if (rk_conf_)
      rd_kafka_conf_destroy(rk_conf_);
  }

  KafkaGlobalConf(const KafkaGlobalConf& other) = delete;
  KafkaGlobalConf& operator=(const KafkaGlobalConf& other) = delete;

  /**
   * Set conf name <--> value
   * 
   * @param name            name of configuration
   * @param value           value of configuration
   * @param errstr          std::string pointer, if Set failed, reason will
   *                        write in *errstr
   *                        
   * @return KafkaConfResult.
   */
  KafkaConfResult Set(const std::string& name, const std::string& value,
                      std::string* errstr);

  /**
   * Get conf value with name.
   * 
   * @param name            name of configuration
   * @param value           configuration to get
   * 
   * @return KafkaConfResult
   */
  KafkaConfResult Get(const std::string& name, std::string* value) const;

  /**
   * Set err callback.
   */
  bool SetErrorCb(void (*err_cb)(rd_kafka_t* rk, int err,
                                 const char* reason,
                                 void* opaque)) {
    if (err_cb == NULL)
      return false;

    rd_kafka_conf_set_error_cb(rk_conf_, err_cb);
    return true;
  }

  /**
   * Set deliver message callback
   */
  bool SetDrMsgCb(void (*dr_msg_cb)(rd_kafka_t* rk,
                                    const rd_kafka_message_t* rkmessage,
                                    void* opaque)) {
    if (dr_msg_cb == NULL)
      return false;

    rd_kafka_conf_set_dr_msg_cb(rk_conf_, dr_msg_cb);
    return true;
  }

 private:
  friend class KafkaProducer;
  friend class KafkaConsumer;

  rd_kafka_conf_t* rk_conf_;
};

// ------------------------------------------------------------------
// KafkaTopicConf

/**
 * Topic Configuration interface.
 * 
 * Holds topic configuration that are passed to KafkaTopicProducer
 * KafkaTopicConsumer.
 */
class KafkaTopicConf {
public:
  /**
   * Static function to create a KafkaTopicConf unique_ptr.
   * 
   * @return std::unique_ptr<KafkaTopicConf>
   */
  static std::unique_ptr<KafkaTopicConf> Init();

  /**
   * Constructor
   * 
   * @param type        configuration type(producer or consumer)
   * @param rk_conf     librdkafka topic configuration raw pointer.
   */
  KafkaTopicConf(rd_kafka_topic_conf_t* rkt_conf = NULL):
      rkt_conf_(rkt_conf) {
    if (!rkt_conf_)
      rkt_conf_ = rd_kafka_topic_conf_new();
  }

  /**
   * Destructor
   * 
   * If rkt_conf_ valid, destory rkt_conf_.
   */
  ~KafkaTopicConf() {
    if (rkt_conf_)
      rd_kafka_topic_conf_destroy(rkt_conf_);
  }

  KafkaTopicConf(const KafkaTopicConf& other) = delete;
  KafkaTopicConf& operator=(const KafkaTopicConf& other) = delete;

  /**
   * Set conf name <--> value.
   * 
   * @param name            name of configuration
   * @param value           value of configuration
   * @param errstr          std::string pointer, if Set failed, reason will
   *                        write in *errstr
   *                        
   * @return KafkaConfResult
   */
  KafkaConfResult Set(const std::string& name, const std::string& value,
                      std::string* errstr);

  /**
   * Get conf value with name.
   * 
   * @param name            name of configuration
   * @param value           configuration to get
   * 
   * @return KafkaConfResult
   */
  KafkaConfResult Get(const std::string& name, std::string* value) const;

  /**
   * Copy function
   * 
   * @return std::unique_ptr<KafkaTopicConf>
   */
  std::unique_ptr<KafkaTopicConf> Copy() const {
    return std::unique_ptr<KafkaTopicConf>(
               new KafkaTopicConf(rd_kafka_topic_conf_dup(rkt_conf_)));
  }

 private:
  friend class KafkaProducer;
  friend class KafkaConsumer;

  rd_kafka_topic_conf_t* rkt_conf_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAFKA_CONF_H_
