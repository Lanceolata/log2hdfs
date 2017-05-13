// Copyright (c) 2017 Lanceolata

#include "kafka/kafka_topic_consumer.h"
#include "easylogging++.h"

namespace log2hdfs {

std::shared_ptr<KafkaTopicConsumer> KafkaTopicConsumer::Init(
    std::shared_ptr<KafkaHandle> handle,
    std::shared_ptr<KafkaTopic> topic,
    std::vector<int32_t> partitions,
    std::vector<int64_t> offsets,
    std::shared_ptr<KafkaConsumeCb> cb) {
  if (!handle || !topic || partitions.empty() || offsets.empty() || !cb) {
    return nullptr;
  }

  for (auto& i : partitions) {
    if (i < 0) {
      return nullptr;
    }
  }

  if (partitions.size() != offsets.size()) {
    return nullptr;
  }

  return std::make_shared<KafkaTopicConsumer>(std::move(handle),
             std::move(topic), std::move(partitions), std::move(offsets),
             std::move(cb));
}

void KafkaTopicConsumer::Start() {
  std::lock_guard<std::mutex> guard(mutex_);
  if (threads_.empty()) {
    running_.store(true);
    std::string topic = topic_->Name();
    for (size_t i = 0; i < partitions_.size(); ++i) {
      int32_t partition = partitions_[i];
      int64_t offset = offsets_[i];
      if (rd_kafka_consume_start(topic_->rkt_, partition, offset) == -1) {
        LOG(WARNING) << "KafkaTopicConsumer Start rd_kafka_consume_start "
                     << "topic[" << topic << "] partition[" << partition
                     << "] offset[" << offset << "] failed "
                     << "with errno[" << errno << "] errnostr["
                     << KafkaErrnoToStr(errno).c_str() << "]";
      } else {
        LOG(INFO) << "KafkaTopicConsumer Start rd_kafka_consume_start "
                  << "topic[" << topic << "] partition[" << partition
                  << "] offset[" << offset << "] success";
        threads_.emplace_back(&KafkaTopicConsumer::StartInternal,
                              this, partition);
      }
    }
  }
}

void KafkaTopicConsumer::Stop() {
  std::lock_guard<std::mutex> guard(mutex_);
  if (!threads_.empty()) {
    std::string topic = topic_->Name();
    for (size_t i = 0; i < partitions_.size(); ++i) {
      int32_t partition = partitions_[i];
      if (rd_kafka_consume_stop(topic_->rkt_, partition) == -1) {
        LOG(WARNING) << "KafkaTopicConsumer Stop rd_kafka_consume_stop "
                     << "topic[" << topic << "] partition[" << partition
                     << "] failed with errno[" << errno << "] errnostr["
                     << KafkaErrnoToStr(errno).c_str() << "]";
      } else {
        LOG(INFO) << "KafkaTopicConsumer Stop rd_kafka_consume_stop "
                  << "topic[" << topic << "] partition[" << partition
                  << "] success";
      }
    }
    running_.store(false);
  }
}

void KafkaTopicConsumer::Join() {
  std::lock_guard<std::mutex> guard(mutex_);
  if (!threads_.empty()) {
    for (size_t i = 0; i < threads_.size(); ++i) {
      if (threads_[i].joinable())
        threads_[i].join();
    }
    threads_.clear();
  }
}

#define CONSUME_BATCH_SIZE 1000
#define CONSUME_BATCH_TIMEOUT_MS 5000
#define POLL_TIMEOUT_MS 5000
#define TRY_TIMES 5

void KafkaTopicConsumer::StartInternal(int32_t partition) {
  const std::string topic = topic_->Name();
  LOG(INFO) << "KafkaTopicConsumer thread topic[" << topic << "] partition["
            << partition << "] created";

  rd_kafka_message_t **messages = static_cast<rd_kafka_message_t **>(
        malloc(CONSUME_BATCH_SIZE * sizeof(rd_kafka_message_t *)));
  if (messages == NULL) {
    LOG(ERROR) << "KafkaTopicConsumer StartInternal malloc for topic["
               << topic << "] partition[" << partition << "] failed";
    if (rd_kafka_consume_stop(topic_->rkt_, partition) == -1) {
      LOG(WARNING) << "KafkaTopicConsumer StartInternal rd_kafka_consume_stop"
                   << " topic[" << topic << "] partition[" << partition
                   << "] failed with errno[" << errno << "] errnostr["
                   << KafkaErrnoToStr(errno).c_str() << "]";
    } else {
      LOG(INFO) << "KafkaTopicConsumer StartInternal rd_kafka_consume_stop"
                << " topic[" << topic << "] partition[" << partition
                << "] success";
    }
    return;
  }

  int times = 0;
  while (true) {
    ssize_t n = rd_kafka_consume_batch(topic_->rkt_, partition,
        CONSUME_BATCH_TIMEOUT_MS, messages, CONSUME_BATCH_SIZE);
    if (n == -1) {
      LOG(ERROR) << "KafkaTopicConsumer StartInternal rd_kafka_consume_batch"
                 << " for topic[" << topic << "] partition[" << partition
                 << "] failed with errno[" << errno << "] errstr["
                 << KafkaErrnoToStr(errno) << "]";
      continue;
    } else if (n == 0) {
      if (!running_.load()) {
        if (++times > TRY_TIMES) {
          break;
        }
      }
      LOG(INFO) << "KafkaTopicConsumer StartInternal topic[" << topic
                << "] partition" << partition << "] no new message";
      continue;
    }

    for (int i = 0; i < n; ++i) {
      rd_kafka_message_t *message = messages[i];
      switch (message->err) {
        case RD_KAFKA_RESP_ERR_NO_ERROR: {
          KafkaMessage msg(message);
          cb_->Consume(msg);
          break;
        }
        case RD_KAFKA_RESP_ERR__PARTITION_EOF:
          LOG(INFO) << "KafkaTopicConsumer StartInternal topic[" << topic
                    << "] partition" << partition << "] end";
          handle_->Poll(POLL_TIMEOUT_MS);
          break;
        default:
          LOG(WARNING) << "KafkaTopicConsumer StartInternal topic[" << topic
                       << "] partition" << partition << "] error["
                       << messages[i]->err << "]";
      }
    }
    handle_->Poll(200);
  }

  handle_->Poll(200);
  free(messages);

  LOG(INFO) << "KafkaTopicConsumer thread topic[" << topic << "] partition["
            << partition << "] exiting";
}

}   // namespace log2hdfs
