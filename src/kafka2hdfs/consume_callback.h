// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_CONSUME_CALLBACK_H_
#define LOG2HDFS_KAFKA2HDFS_CONSUME_CALLBACK_H_

#include <string>
#include <memory>
#include "kafka/kafka_topic_consumer.h"
#include "util/fp_cache.h"
#include "util/optional.h"

namespace log2hdfs {

class PathFormat;
class TopicConf;

// ------------------------------------------------------------------
// ConsumeCallback

class ConsumeCallback : public KafkaConsumeCb {
 public:
  enum Type {
    kNone,
    kV6,
    kEf,
    kReport,
    kDebug
  };

  static Optional<ConsumeCallback::Type> ParseType(
      const std::string& type);

  static std::shared_ptr<KafkaConsumeCb> Init(
      ConsumeCallback::Type type,
      std::shared_ptr<TopicConf> conf,
      std::shared_ptr<PathFormat> format,
      std::shared_ptr<FpCache> cache);

  ConsumeCallback(const std::string& dir,
                  std::shared_ptr<PathFormat> format,
                  std::shared_ptr<FpCache> cache):
      dir_(dir), format_(std::move(format)), cache_(std::move(cache)) {}

  virtual ~ConsumeCallback() {}

  virtual void Consume(const KafkaMessage& msg) = 0;

 protected:
  std::string dir_;
  std::shared_ptr<PathFormat> format_;
  std::shared_ptr<FpCache> cache_;
};

// ------------------------------------------------------------------
// V6ConsumeCallback

class V6ConsumeCallback : public ConsumeCallback {
 public:
  static std::shared_ptr<V6ConsumeCallback> Init(
        std::shared_ptr<TopicConf> conf,
        std::shared_ptr<PathFormat> format,
        std::shared_ptr<FpCache> cache);

  V6ConsumeCallback(const std::string& dir,
                    std::shared_ptr<PathFormat> format,
                    std::shared_ptr<FpCache> cache):
      ConsumeCallback(dir, std::move(format), std::move(cache)) {}

  V6ConsumeCallback(const V6ConsumeCallback& other) = delete;
  V6ConsumeCallback& operator=(const V6ConsumeCallback& other) = delete;

  void Consume(const KafkaMessage& msg);
};

// ------------------------------------------------------------------
// EfConsumeCallback

typedef V6ConsumeCallback EfConsumeCallback;

// ------------------------------------------------------------------
// ReportConsumeCallback

class ReportConsumeCallback : public ConsumeCallback {
 public:
  static std::shared_ptr<ReportConsumeCallback> Init(
        std::shared_ptr<TopicConf> conf,
        std::shared_ptr<PathFormat> format,
        std::shared_ptr<FpCache> cache);

  ReportConsumeCallback(const std::string& dir,
                        std::shared_ptr<PathFormat> format,
                        std::shared_ptr<FpCache> cache):
      ConsumeCallback(dir, std::move(format), std::move(cache)) {}

  ReportConsumeCallback(const ReportConsumeCallback& other) = delete;
  ReportConsumeCallback& operator=(
      const ReportConsumeCallback& other) = delete;

  void Consume(const KafkaMessage& msg);
};

// ------------------------------------------------------------------
// DebugConsumeCallback
 
class DebugConsumeCallback : public ConsumeCallback {
 public:
  static std::shared_ptr<DebugConsumeCallback> Init(
        std::shared_ptr<TopicConf> conf,
        std::shared_ptr<PathFormat> format,
        std::shared_ptr<FpCache> cache);

  DebugConsumeCallback(const std::string& dir,
                        std::shared_ptr<PathFormat> format,
                        std::shared_ptr<FpCache> cache):
      ConsumeCallback(dir, std::move(format), std::move(cache)) {}

  DebugConsumeCallback(const DebugConsumeCallback& other) = delete;
  DebugConsumeCallback& operator=(
      const DebugConsumeCallback& other) = delete;

  void Consume(const KafkaMessage& msg);
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_CONSUME_CALLBACK_H_
