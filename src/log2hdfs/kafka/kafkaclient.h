#ifndef LOG2KAFKA_KAFKA_KAFKACLIENT_H
#define LOG2KAFKA_KAFKA_KAFKACLIENT_H

#include <map>

#ifdef __cplusplus
extern "C" {
#endif
#include "rdkafka.h"
#ifdef __cplusplus
}
#endif

#include "log2kafka/util/logger.h"

namespace log2kafka {

namespace kafka {

class RdKafkaClient {
 public:

  static shared_ptr<KafkaClient> Make(const char *brokers, 
                                      std::map<std::string, std::string> confs,
                                      shared_ptr<Logger> logger);
 private:

  kafkaclient(const char *brokers, std::map<std::string, std::string> confs, 
              shared_ptr<Logger> logger);

  rd_kafka_t *rk;

};

class RdKafkaProducer: public kafkaclient {

};

class RdKafkaConsumer: public kafkaclient {

};

}   // namespace kafka

}   // namespace log2kafka

#endif  // LOG2KAFKA_KAFKA_KAFKACLIENT_H


