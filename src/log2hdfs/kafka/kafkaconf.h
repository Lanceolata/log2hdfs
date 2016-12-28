#ifndef LOG2KAFKA_KAFKA_KAFKACONF_H
#define LOG2KAFKA_KAFKA_KAFKACONF_H

#include <map>

#ifdef __cplusplus
extern "C" {
#endif
#include "rdkafka.h"
#ifdef __cplusplus
}
#endif

#include "log2kafka/util/logger.h"

namespace log2hdfs {

namespace kafka {

class GlobalConf {
 public:
  static unique_ptr<GlobalConf> Make();
  static unique_ptr<GlobalConf> Make(
          std::map<std::string, std::string>& confs);
  
  ~GlobalConf();

  bool conf_set(const char *name, const char *value);

  void set_dr_msg_cb(void (*dr_msg_cb) (kafka_producer_t *rk,
                                        const kafka_message_t *rkmessage,
                                        void *opaque));

  void set_consume_cb(void (*consume_cb) (rd_kafka_message_t *rkmessage,
                                          void *opaque));

  void set_error_cb(void  (*error_cb) (rd_kafka_t *rk, int err,
                                       const char *reason,
                                       void *opaque));

  // TODO other callbacks

 private:

  GlobalConf(rd_kafka_conf_t *conf);

  rd_kafka_conf_t *conf_;
};

class TopicConf {
 public:
  static unique_ptr<TopicConf> Make();
  static unique_ptr<TopicConf> Make(
          std::map<std::string, std::string>& confs);

  ~TopicConf();

  void set_partitioner_cb(int32_t (*partitioner) (const rd_kafka_topic_t *rkt,
                                                  const void *keydata,
                                                  size_t keylen,
                                                  int32_t partition_cnt,
                                                  void *rkt_opaque,
                                                  void *msg_opaque));
 
 private:
  TopicConf(rd_kafka_topic_conf_t *topic_conf);

  rd_kafka_topic_conf_t *topic_conf_;
};

}   // namespace kafka

}   // namespace log2hdfs

#endif  // LOG2KAFKA_KAFKA_KAFKACONF_H
