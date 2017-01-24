#ifndef LOG2KAFKA_KAFKA_CONF_H
#define LOG2KAFKA_KAFKA_CONF_H

#ifdef __cplusplus
extern "C" {
#endif
#include "rdkafka.h"
#ifdef __cplusplus
}
#endif

#include <memory>

namespace log2hdfs {

namespace kafka {

class GlobalConf;
typedef std::unique_ptr<GlobalConf> GlobalConfPtr;

class GlobalConf {
 public:

  enum ConfResult {
    CONF_UNKNOWN = -2,  /**< Unknown configuration property */
    CONF_INVALID = -1,  /**< Invalid configuration value */
    CONF_OK = 0         /**< Configuration property was succesfully set */
  };

  static GlobalConf create();

  virtual ~GlobalConf() {}

  virtual ConfResult set(const std::string& name, const std::string& value,
                         std::string& errstr) = 0;

};

class TopicConf;
typedef std::unique_ptr<TopicConf> TopicConfPtr;

class TopicConf {
 public:

  
  
  static TopicConfPtr create();


};

}   // namespace kafka

}   // namespace log2hdfs

#endif  // LOG2KAFKA_KAFKA_RDKAFKACONF_H
