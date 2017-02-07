// Copyright (c) 2017 Lanceolata

#ifndef LOG2KAFKA_KAFKA_CONF_H_
#define LOG2KAFKA_KAFKA_CONF_H_

#ifdef __cplusplus
extern "C" {
#endif
#include "rdkafka.h"
#ifdef __cplusplus
}
#endif

#include <string>
#include <memory>

namespace log2hdfs {

namespace kafka {

// Conf Set() result code
enum ConfResult {
  kConfEmpty = -3,      // Empty configuration property
  kConfUnknown = -2,    // Unknown configuration property
  kConfInvalid = -1,    // Invalid configuration value
  kConfOk = 0           // Configuration property was succesfully set
};

// ------------------------------------------------------------------
// GlobalConf

class GlobalConf {
 public:
  // Configuration object type
  enum ConfType {
    kConfProducer,    // Global producer default configuration
    kConfConsumer    // Global consumer default configuration
  };

  static std::unique_ptr<GlobalConf> Create(GlobalConf::ConfType type);

  virtual ~GlobalConf() {}

  virtual std::unique_ptr<GlobalConf> Copy() const = 0;

  virtual ConfResult Set(const std::string &name, const std::string &value,
                         std::string *errstr) = 0;

  virtual ConfResult Get(const std::string &name,
                         std::string *value) const = 0;

  virtual rd_kafka_conf_t *conf() = 0;
};

// ------------------------------------------------------------------
// TopicConf
/*
class TopicConf {
 public:
  // Configuration object type
  enum ConfType {
    kConfProducer,  // Topic producer default configuration
    kConfConsumer   // Topic consumer default configuration
  };

  static std::unique_ptr<TopicConf> Create(GlobalConf::ConfType type);

  virtual ~TopicConf() {}

  virtual std::unique_ptr<TopicConf> Copy() const = 0;

  virtual ConfResult Set(const std::string &name, const std::string &value,
                         std::string *errstr) = 0;

  virtual ConfResult Get(const std::string &name,
                         std::string *value) const = 0;

  virtual rd_kafka_topic_conf_t *conf() = 0;
};
*/
}   // namespace kafka

}   // namespace log2hdfs

#endif  // LOG2KAFKA_KAFKA_CONF_H_
