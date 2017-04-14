// Copyright (c) 2017 Lanceolata

#include "kafka/kafka_topic.h"
#include <utility>
#include "kafka/kafka_partition_consumer.h"
#include "util/logger.h"

namespace log2hdfs {

std::shared_ptr<KafkaTopic> KafkaTopic::Init(
    rd_kafka_topic_t *rkt, std::shared_ptr<KafkaHandle> handle) {
  if (!rkt || !handle || !handle.get()) {
    return nullptr;
  }
  return std::make_shared<KafkaTopic>(rkt, std::move(handle));
}

std::shared_ptr<KafkaPartitionConsumer> KafkaTopic::CreatePartitionConsumer(
    int32_t partition, int64_t offset,
    std::shared_ptr<KafkaPartitionConsumeCb> cb) {
  std::lock_guard<std::mutex> guard(mutex_);

  auto it = partition_consumers_.find(partition);
  if (it != partition_consumers_.end()) {
    Log(LogLevel::kLogWarn, "PartitionConsumer partition[%d] already create",
        partition);
    return it->second;
  }

  std::shared_ptr<KafkaPartitionConsumer> kpc;
  kpc = KafkaPartitionConsumer::Init(handle_, topic_handle_,
                                     partition, offset, cb);
  if (!kpc) {
    Log(LogLevel::kLogError, "KafkaPartitionConsumer init failed "
        "topic[%d] partition[%d] offset[%ld]", topic_handle_->Name().c_str(),
        partition, offset);
    return kpc;
  } else {
    partition_consumers_.insert(std::make_pair(partition, kpc));
    Log(LogLevel::kLogInfo, "KafkaPartitionConsumer init success "
        "topic[%d] partition[%d] offset[%ld]", topic_handle_->Name().c_str(),
        partition, offset);
  }
 
  return kpc;
}

bool KafkaTopic::StartAllPartitionConsumer() {
  std::lock_guard<std::mutex> guard(mutex_);
  if (partition_consumers_.empty()) {
    Log(LogLevel::kLogWarn, "partition_consumers_ empty topic[%s]",
        topic_handle_->Name().c_str());
    return false;
  }

  auto it = partition_consumers_.begin();
  for (; it != partition_consumers_.end(); ++it) {
    it->second->Start();
  }
  return true;
}

bool KafkaTopic::StopAllPartitionConsumer() {
  std::lock_guard<std::mutex> guard(mutex_);
  if (partition_consumers_.empty()) {
    Log(LogLevel::kLogWarn, "partition_consumers_ empty topic[%s]",
        topic_handle_->Name().c_str());
    return false;
  }

  auto it = partition_consumers_.begin();
  for (; it != partition_consumers_.end(); ++it) {
    it->second->Stop();
  }
  return true;
}

}   // namespace log2hdfs
