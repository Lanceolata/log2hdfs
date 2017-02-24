// Copyright (c) 2017 Lanceolata

#include "kafka/kafka_consumer.h"
#include "kafka/kafka_conf.h"
#include "kafka/kafka_topic.h"
#include "kafka/kafka_message.h"
#include "kafka/kafka_error.h"
#include "util/logger.h"

namespace log2hdfs {

// ------------------------------------------------------------------
// TopicPartitionConsumer

std::shared_ptr<TopicPartitionConsumer> TopicPartitionConsumer::Init(
        std::shared_ptr<Handle> handle, std::shared_ptr<Topic> topic,
        int32_t partition, int64_t offset, std::shared_ptr<ConsumeCb> cb) {
  if (!handle || !topic || partition < 0) {
    return nullptr;
  }

  return std::shared_ptr<TopicPartitionConsumer>(
          new TopicPartitionConsumer(handle, topic, partition, offset, cb));
}

#define CONSUME_BATCH_SIZE 1000
#define CONSUME_BATCH_TIMEOUT_MS 5000
#define POLL_TIMEOUT_MS 5000

bool TopicPartitionConsumer::Start(std::string *errstr) {
  running = true;
  rd_kafka_message_t **messages = static_cast<rd_kafka_message_t **>(malloc(
          CONSUME_BATCH_SIZE * sizeof(rd_kafka_message_t *)));
  if (messages == NULL) {
    *errstr = "calloc failed for messages to be consumed";
    running = false;
    return false;
  }

  if (rd_kafka_consume_start(topic_->rkt_, partition_, offset_) == -1) {
    char errbuf[256];
    snprintf(errbuf, sizeof(errbuf), "rd_kafka_consume_start "
             "failed with errno[%d]", errno);
    *errstr = errbuf;
    running = false;
    return false;
  }

  const std::string topic_name = Name();

  while (true) {
    ssize_t n = rd_kafka_consume_batch(topic_->rkt_, partition_,
                                       CONSUME_BATCH_TIMEOUT_MS, messages,
                                       CONSUME_BATCH_SIZE);

    if (n == -1) {
      Log(LogLevel::kLogError, "rd_kafka_consume_batch "
          "failed with errno[%d] errstr[%s]", errno,
          ErrorToStr(ErrnoToError(errno)).c_str());
      continue;
    } else if (n == 0) {
      if (!running) {
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
          std::unique_ptr<Message> msg = Message::Init(message);
          cb_->Consume(msg.get());
          break;
        }
        case RD_KAFKA_RESP_ERR__PARTITION_EOF:
          Log(LogLevel::kLogInfo, "topic[%s]-partition[%d] end",
              topic_name.c_str(), partition_);
          Poll(POLL_TIMEOUT_MS);
          break;
        default:
          Log(LogLevel::kLogWarn, "topic[%s]-partition[%d] "
              "error[%d][%s]", topic_name.c_str(), partition_,
              messages[i]->err, message->payload);
      }
      std::unique_ptr<Message> msg = Message::Init(message);
      cb_->Consume(msg.get());
    }
    Poll();
  }

  return true;
}

void TopicPartitionConsumer::Stop() {
  if (running) {
    if (rd_kafka_consume_stop(topic_->rkt_, partition_) == -1) {
        Log(LogLevel::kLogError, "rd_kafka_consume_stop "
            "failed with errno[%d], errstr[%s]", errno,
            ErrorToStr(ErrnoToError(errno)).c_str());
    }
    running = false;
  }
}

// ------------------------------------------------------------------
// Consumer

std::unique_ptr<Consumer> Consumer::Init(GlobalConf *conf,
                                         std::string *errstr) {
  if (!conf || !conf->rk_conf_) {
    if (errstr) {
      *errstr = "Requires log2hdfs::kafka::GlobalConf pointer";
    }
    return nullptr;
  }

  char errbuf[512];
  rd_kafka_conf_t *rk_conf = rd_kafka_conf_dup(conf->rk_conf_);
  rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_CONSUMER, rk_conf,
                                errbuf, sizeof(errbuf));
  if (!rk) {
    *errstr = errbuf;
    rd_kafka_conf_destroy(rk_conf);
    return nullptr;
  }

  return std::unique_ptr<Consumer>(new Consumer(rk));
}

std::shared_ptr<Topic> Consumer::CreateTopic(const std::string &topic,
                                             TopicConf *conf,
                                             std::string *errstr) {
  if (topic.empty() || !conf || !conf->rkt_conf_) {
    *errstr = "Requires topic name and log2hdfs::kafka::TopicConf pointer";
    return nullptr;
  }

  rd_kafka_topic_conf_t *rkt_conf = rd_kafka_topic_conf_dup(conf->rkt_conf_);
  rd_kafka_topic_t *rkt = rd_kafka_topic_new(handle_->rk_, topic.c_str(),
                                             rkt_conf);

  if (!rkt) {
    *errstr = ErrorToStr(ErrnoToError(errno));
    rd_kafka_topic_conf_destroy(rkt_conf);
    return nullptr;
  }
  return Topic::Init(rkt);
}

}   // namespace log2hdfs
