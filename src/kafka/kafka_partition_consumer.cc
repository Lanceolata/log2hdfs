// Copyright (c) 2017 Lanceolata

#include "kafka/kafka_partition_consumer.h"
#include "kafka/kafka_handle.h"
#include "kafka/kafka_topic.h"
#include "kafka/kafka_message.h"
#include "kafka/kafka_error.h"
#include "util/logger.h"

namespace log2hdfs {

std::shared_ptr<KafkaPartitionConsumer> KafkaPartitionConsumer::Init(
    std::shared_ptr<KafkaHandle> handle,
    std::shared_ptr<KafkaTopic> topic,
    int32_t partition, int64_t offset,
    std::shared_ptr<KafkaPartitionConsumerCb> cb) {
  if (!handle || !topic || partition < 0) {
    return nullptr;
  }
  return std::make_shared<KafkaPartitionConsumer>(
             handle, topic, partition, offset, cb);
}

#define CONSUME_BATCH_SIZE 1000
#define CONSUME_BATCH_TIMEOUT_MS 5000
#define POLL_TIMEOUT_MS 5000

void KafkaPartitionConsumer::Start() {
  const std::string topic_name = topic_->Name();
  Log(LogLevel::kLogInfo, "Consume Thread topic[%s] partition[%d] created",
      topic_name.c_str(), partition_);

  running_ = true;
  rd_kafka_message_t **messages = static_cast<rd_kafka_message_t **>(
      malloc(CONSUME_BATCH_SIZE * sizeof(rd_kafka_message_t *)));
  if (messages == NULL) {
    Log(LogLevel::kLogError, "calloc failed for messages to be consumed");
    running_ = false;
    return;
  }

  if (rd_kafka_consume_start(topic_->rkt_, partition_, offset_) == -1) {
    Log(LogLevel::kLogError, "rd_kafka_consume_start failed "
        "with errno[%d]", errno);
    running_ = false;
    return;
  }

  while (true) {
    ssize_t n = rd_kafka_consume_batch(topic_->rkt_, partition_,
                                       CONSUME_BATCH_TIMEOUT_MS, messages,
                                       CONSUME_BATCH_SIZE);
    if (n == -1) {
      Log(LogLevel::kLogError, "rd_kafka_consume_batch "
          "failed with errno[%d] errstr[%s]", errno,
          KafkaErrnoToStr(errno).c_str());
      continue;
    } else if (n == 0) {
      if (!running_) {
        Log(LogLevel::kLogInfo, "topic[%s]-partition[%d] ",
            "stopping", topic_name.c_str(), partition_);
        break;
      }
      Log(LogLevel::kLogInfo, "topic[%s]-partition[%d] "
          "no new message", topic_name.c_str(), partition_);
      continue;
    }

    for (int i = 0; i < n; ++i) {
      rd_kafka_message_t *message = messages[i];
      switch (message->err) {
        case RD_KAFKA_RESP_ERR_NO_ERROR: {
          std::unique_ptr<KafkaMessage> msg = KafkaMessage::Init(message);
          cb_->Consume(msg.get());
          break;
        }
        case RD_KAFKA_RESP_ERR__PARTITION_EOF:
          Log(LogLevel::kLogInfo, "topic[%s]-partition[%d] end",
              topic_name.c_str(), partition_);
          handle_->Poll(POLL_TIMEOUT_MS);
          break;
        default:
          Log(LogLevel::kLogWarn, "topic[%s]-partition[%d] "
              "error[%d][%s]", topic_name.c_str(), partition_,
              messages[i]->err, message->payload);
      }
    }
    handle_->Poll();
  }

  Log(LogLevel::kLogInfo, "Consume Thread topic[%s] partition[%d] exiting",
      topic_name.c_str(), partition_);
}

void KafkaPartitionConsumer::Stop() {
  if (running_) {
    if (rd_kafka_consume_stop(topic_->rkt_, partition_) == -1) {
      Log(LogLevel::kLogError, "rd_kafka_consume_stop "
          "failed with errno[%d], errstr[%s]", errno,
          KafkaErrnoToStr(errno).c_str());
    }
    running_ = false;
  }
}

}   // namespace log2hdfs