// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_CONSUME_CALLBACK_H_
#define LOG2HDFS_KAFKA2HDFS_CONSUME_CALLBACK_H_

#include <memory>
#include <string>
#include "kafka/kafka_consumer.h"
#include "util/fp_cache.h"

namespace log2hdfs {

class PathFormat;
class TopicPartitionConsumer;

class ReportConsumeCallback : public ConsumeCb {
 public:
  static std::shared_ptr<ConsumeCb> Init(std::shared_ptr<PathFormat> format,
                                         std::shared_ptr<FpCache> cache);

  ReportConsumeCallback(const ReportConsumeCallback &other) = delete;
  ReportConsumeCallback &operator=(
      const ReportConsumeCallback &other) = delete;

  ~ReportConsumeCallback() {}

  void Consume(Message *msg);

 private:
  explicit ReportConsumeCallback(std::shared_ptr<PathFormat> format,
                                 std::shared_ptr<FpCache> cache):
      format_(format), cache_(cache) {}

  std::shared_ptr<PathFormat> format_;
  std::shared_ptr<FpCache> cache_;
};

class V6ConsumeCallback : public ConsumeCb {
 public:
  static std::shared_ptr<ConsumeCb> Init(std::shared_ptr<PathFormat> format,
                                         std::shared_ptr<FpCache> cache);

  V6ConsumeCallback(const V6ConsumeCallback &other) = delete;
  V6ConsumeCallback &operator=(const V6ConsumeCallback &other) = delete;

  ~V6ConsumeCallback() {}

  void Consume(Message *msg);

 private:
  explicit V6ConsumeCallback(std::shared_ptr<PathFormat> format,
                             std::shared_ptr<FpCache> cache):
      format_(format), cache_(cache) {}

  std::shared_ptr<PathFormat> format_;
  std::shared_ptr<FpCache> cache_;
};

class EfConsumeCallback : public ConsumeCb {
 public:
  static std::shared_ptr<ConsumeCb> Init(std::shared_ptr<PathFormat> format,
                                         std::shared_ptr<FpCache> cache);

  EfConsumeCallback(const EfConsumeCallback &other) = delete;
  EfConsumeCallback &operator=(const EfConsumeCallback &other) = delete;

  ~EfConsumeCallback() {}

  void Consume(Message *msg);

 private:
  explicit EfConsumeCallback(std::shared_ptr<PathFormat> format,
                             std::shared_ptr<FpCache> cache):
      format_(format), cache_(cache) {}

  std::shared_ptr<PathFormat> format_;
  std::shared_ptr<FpCache> cache_;
};

// ------------------------------------------------------------------
// ConsumeThreadWrap

extern void ConsumeThreadWrap(
    std::shared_ptr<TopicPartitionConsumer> tp_consumer);

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_CONSUME_CALLBACK_H_
