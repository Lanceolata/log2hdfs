// Copyright (c) 2017 Lanceolata

#include "kafka2hdfs/consume_callback.h"
#include <string.h>
#include <stdio.h>
#include "kafka2hdfs/path_format.h"
#include "kafka/kafka_consumer.h"
#include "kafka/kafka_message.h"
#include "util/logger.h"

namespace log2hdfs {

// ------------------------------------------------------------------
// ReportConsumeCallback

std::shared_ptr<ConsumeCb> ReportConsumeCallback::Init(
    std::shared_ptr<PathFormat> format, std::shared_ptr<FpCache> cache) {
  if (!format || !format.get() || !cache || !cache.get()) {
    return nullptr;
  }
  return std::shared_ptr<ConsumeCb>(new ReportConsumeCallback(format, cache));
}

void ReportConsumeCallback::Consume(Message *msg) {
  std::string topic = msg->TopicName();
  char *payload = static_cast<char *>(msg->Payload());
  size_t len = msg->Len();

  Optional<std::string> local_path = format_->BuildLocalPathFromMsg(msg);
  if (!local_path.valid()) {
    Log(LogLevel::kLogWarn, "Build local path for topic[%s] "
        "from msg[%s] failed", topic.c_str(), payload);
    return;
  }
  std::shared_ptr<FILE> ptr = cache_->Get(local_path.value());
  if (!ptr | !ptr.get()) {
    Log(LogLevel::kLogError, "Get cache fp failed with path[%s]",
        local_path.value().c_str());
    return;
  }

  char *pt = strchr(payload, '\t') + 1;
  len = len - (pt - payload);
  payload = pt;

  std::unique_ptr<char[]> temp(new char[len + 1]);
  char *p = temp.get();
  memcpy(p, payload, len);
  temp[len] = '\n';

  size_t n = fwrite(p, 1, len + 1, ptr.get());
  if (n != len + 1) {
    Log(LogLevel::kLogError, "fwrite[%s] might fail: "
        "written[%lu] expected[%lu]", local_path.value().c_str(),
        n, len + 1);
  }
}

// ------------------------------------------------------------------
// V6ConsumeCallback

std::shared_ptr<ConsumeCb> V6ConsumeCallback::Init(
    std::shared_ptr<PathFormat> format, std::shared_ptr<FpCache> cache) {
  if (!format || !format.get() || !cache || !cache.get()) {
    return nullptr;
  }
  return std::shared_ptr<ConsumeCb>(new V6ConsumeCallback(format, cache));
}

void V6ConsumeCallback::Consume(Message *msg) {
  std::string topic = msg->TopicName();
  char *payload = static_cast<char *>(msg->Payload());
  size_t len = msg->Len();

  Optional<std::string> local_path = format_->BuildLocalPathFromMsg(msg);
  if (!local_path.valid()) {
    Log(LogLevel::kLogWarn, "Build local path for topic[%s] "
        "from msg[%s] failed", topic.c_str(), payload);
    return;
  }
  std::shared_ptr<FILE> ptr = cache_->Get(local_path.value());
  if (!ptr | !ptr.get()) {
    Log(LogLevel::kLogError, "Get cache fp failed with path[%s]",
        local_path.value().c_str());
    return;
  }

  std::unique_ptr<char[]> temp(new char[len + 1]);
  char *p = temp.get();
  memcpy(p, payload, len);
  temp[len] = '\n';

  size_t n = fwrite(p, 1, len + 1, ptr.get());
  if (n != len + 1) {
    Log(LogLevel::kLogError, "fwrite[%s] might fail: "
        "written[%lu] expected[%lu]", local_path.value().c_str(),
        n, len + 1);
  }
}

// ------------------------------------------------------------------
// EfConsumeCallback

std::shared_ptr<ConsumeCb> EfConsumeCallback::Init(
    std::shared_ptr<PathFormat> format, std::shared_ptr<FpCache> cache) {
  if (!format || !format.get() || !cache || !cache.get()) {
    return nullptr;
  }
  return std::shared_ptr<ConsumeCb>(new EfConsumeCallback(format, cache));
}

void EfConsumeCallback::Consume(Message *msg) {
  std::string topic = msg->TopicName();
  char *payload = static_cast<char *>(msg->Payload());
  size_t len = msg->Len();

  Optional<std::string> local_path = format_->BuildLocalPathFromMsg(msg);
  if (!local_path.valid()) {
    Log(LogLevel::kLogWarn, "Build local path for topic[%s] "
        "from msg[%s] failed", topic.c_str(), payload);
    return;
  }
  std::shared_ptr<FILE> ptr = cache_->Get(local_path.value());
  if (!ptr | !ptr.get()) {
    Log(LogLevel::kLogError, "Get cache fp failed with path[%s]",
        local_path.value().c_str());
    return;
  }

  std::unique_ptr<char[]> temp(new char[len + 1]);
  char *p = temp.get();
  memcpy(p, payload, len);
  temp[len] = '\n';

  size_t n = fwrite(p, 1, len + 1, ptr.get());
  if (n != len + 1) {
    Log(LogLevel::kLogError, "fwrite[%s] might fail: "
        "written[%lu] expected[%lu]", local_path.value().c_str(),
        n, len + 1);
  }
}

// ------------------------------------------------------------------
// ConsumeThreadWrap

void ConsumeThreadWrap(std::shared_ptr<TopicPartitionConsumer> tp_consumer) {
    std::string topic_name = tp_consumer->Name();
    int partition = tp_consumer->partition();
    Log(LogLevel::kLogInfo, "Consume Thread topic[%s] partition[%d] created",
        topic_name.c_str(), partition);

    std::string res;
    if (!tp_consumer->Start(&res)) {
      Log(LogLevel::kLogError, "Start consumer topic[%s] partition[%d] "
          "failed with err[%s]", topic_name.c_str(), partition, res.c_str());
    }
    Log(LogLevel::kLogInfo, "Consume Thread topic[%s] partition[%d] exiting",
        topic_name.c_str(), partition);
}

}   // namespace log2hdfs
