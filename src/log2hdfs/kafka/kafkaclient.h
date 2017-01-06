#ifndef LOG2KAFKA_KAFKA_KAFKACLIENT_H
#define LOG2KAFKA_KAFKA_KAFKACLIENT_H

#include <string>

namespace log2kafka {

namespace kafka {

typedef rd_kafka_message_t kafka_message_t;

class KafkaClient {
 public:
  
  KafkaClient(LoggerPtr logger): logger_(logger) {}
  virtual ~KafkaClient() {}

  static const char *get_topic_from_msg(const kafka_message_t *msg);
  static const char *get_err_from_msg(const kafka_message_t *msg);
  static const char *get_payload_from_msg(const kafka_message_t *msg);

 protected:

  LoggerPtr logger_;
}

class KafkaProducer: public KafkaClient {
 public:


 private:
};

class KafkaConsumer: public KafkaClient {
 public:

};

}   // kafka

}   // namespace log2kafka

#endif  // LOG2KAFKA_KAFKA_KAFKACLIENT_H
