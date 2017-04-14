// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_CONSUME_CALLBACK_H_
#define LOG2HDFS_KAFKA2HDFS_CONSUME_CALLBACK_H_

#include <string>
#include <memory>
#include "kafka/kafka_partition_consumer.h"
#include "util/fp_cache.h"
#include "util/optional.h"

namespace log2hdfs {

class LogFormat;

class ConsumeCallback : public KafkaPartitionConsumeCb {
 public:
  enum Type {
    kReport,
    kV6,
    kEf,
    kNone
  };

  static Optional<ConsumeCallback::Type> GetTypeFromString(
      const std::string &type);

  static std::shared_ptr<KafkaPartitionConsumeCb> Init(
      std::shared_ptr<LogFormat> format, std::shared_ptr<FpCache> cache);

  ConsumeCallback(std::shared_ptr<LogFormat> format,
                  std::shared_ptr<FpCache> cache):
      log_format_(format), fp_cache_(cache) {}

  virtual ~ConsumeCallback() {}

  virtual void Consume(const KafkaMessage &msg) = 0;

 private:
  std::shared_ptr<LogFormat> log_format_;
  std::shared_ptr<FpCache> fp_cache_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_CONSUME_CALLBACK_H_
