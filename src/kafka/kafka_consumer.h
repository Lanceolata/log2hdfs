// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA_KAFKA_CONSUMER_H_
#define LOG2HDFS_KAFKA_KAFKA_CONSUMER_H_

#include <string>
#include <utility>
#include <memory>
#include <mutex>
#include <unordered_map>

#ifdef __cplusplus
extern "C" {
#endif
#include "librdkafka/rdkafka.h"
#ifdef __cplusplus
}
#endif

#include "kafka/kafka_handle.h"
#include "kafka/kafka_error.h"

namespace log2hdfs {

class GlobalConf;
class TopicConf;
class Topic;
class Message;

class ConsumeCb {
 public:
  virtual void Consume(Message *msg) = 0;

  virtual ~ConsumeCb() {}
};

class TopicPartitionConsumer {
 public:
  static std::shared_ptr<TopicPartitionConsumer> Init(
          std::shared_ptr<Handle> handle,
          std::shared_ptr<Topic> topic,
          int32_t partition, int64_t offset,
          std::shared_ptr<ConsumeCb> cb);

  TopicPartitionConsumer() {}

  const std::string Name() const {
    return handle_->Name();
  }

  int Poll(int timeout_ms = 0) {
    return handle_->Poll(timeout_ms);
  }

  bool Start(std::string *errstr);

  void Stop();

  int32_t partition() {
    return partition_;
  }

  int64_t offset() {
    return offset_;
  }

  void set_offset(int64_t offset) {
    offset_ = offset;
  }

  std::shared_ptr<ConsumeCb> cb() {
    return cb_;
  }

  bool set_cb(std::shared_ptr<ConsumeCb> cb) {
    if (!cb) {
      return false;
    }
    cb_ = cb;
    return true;
  }

 private:
  explicit TopicPartitionConsumer(std::shared_ptr<Handle> handle,
                                  std::shared_ptr<Topic> topic,
                                  int32_t partition, int64_t offset,
                                  std::shared_ptr<ConsumeCb> cb):
      handle_(handle), topic_(topic), partition_(partition), offset_(offset),
      cb_(cb) {}

  std::shared_ptr<Handle> handle_;
  std::shared_ptr<Topic> topic_;
  int32_t partition_;
  int64_t offset_;
  std::shared_ptr<ConsumeCb> cb_;
  bool running = false;
};

class Consumer {
 public:
  static std::unique_ptr<Consumer> Init(GlobalConf *conf,
                                        std::string *errstr);

  ~Consumer() {}

  const std::string Name() const {
    return handle_->Name();
  }

  const std::string MemberId() const {
    return handle_->MemberId();
  }

  int Poll(int timeout_ms = 0) {
    return handle_->Poll(timeout_ms);
  }

  std::shared_ptr<Topic> CreateTopic(const std::string &topic,
                                     TopicConf *conf,
                                     std::string *errstr);

  std::shared_ptr<TopicPartitionConsumer> CreateTopicPartitionConsumer(
          std::shared_ptr<Topic> topic, int32_t partition, int64_t offset,
          std::shared_ptr<ConsumeCb> cb) {
    std::shared_ptr<TopicPartitionConsumer> tpc;
    tpc = TopicPartitionConsumer::Init(handle_, topic, partition,
                                       offset, cb);
    if (tpc) {
      std::lock_guard<std::mutex> guard(mutex_);
      consumers_.insert(std::make_pair(topic, tpc));
    }
    return tpc;
  }

  void StopAll(int milli = 5000) {
    std::lock_guard<std::mutex> guard(mutex_);
    for (auto it = consumers_.begin(); it != consumers_.end(); ++it) {
      it->second->Stop();
    }
    while (handle_->OutqLen() > 0) {
      handle_->Poll(milli);
    }
  }

 private:
  explicit Consumer(rd_kafka_t *rk): handle_(Handle::Init(rk)) {}

  std::mutex mutex_;
  std::shared_ptr<Handle> handle_;
  std::unordered_multimap<std::shared_ptr<Topic>,
      std::shared_ptr<TopicPartitionConsumer> > consumers_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA_KAFKA_CONSUMER_H_
