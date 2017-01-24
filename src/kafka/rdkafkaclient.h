#ifndef LOG2KAFKA_KAFKA_RDKAFKACLIENT_H
#define LOG2KAFKA_KAFKA_RDKAFKACLIENT_H

#include <string>
#include "logger.h"

namespace log2kafka {

namespace kafka {

class RdKafkaClient {
 public:
  
  RdKafkaClient(LoggerPtr logger): logger_(logger) {}
  virtual ~RdKafkaClient() {} 

  const char *get_topic_from_msg(const kafka_message_t *msg);
  const char *get_err_from_msg(const kafka_message_t *msg);
  const char *get_payload_from_msg(const kafka_message_t *msg);

 protected:

  LoggerPtr logger_;
};

class RdKafkaProducer {

};

class RdKafkaConsumer {

};

}   // namespace kafka

}   // namespace log2kafka

#endif  // LOG2KAFKA_KAFKA_RDKAFKACLIENT_H
