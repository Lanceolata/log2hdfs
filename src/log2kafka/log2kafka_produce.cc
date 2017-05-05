// Copyright (c) 2017 Lanceolata

#include "log2kafka/log2kafka_produce.h"
#include <stdlib.h>
#include <fstream>
#include "log2kafka/log2kafka_topic_conf.h"
#include "log2kafka/log2kafka_offset_table.h"
#include "log2kafka/log2kafka_errmsg_handle.h"
#include "kafka/kafka_producer.h"
#include "kafka/kafka_topic_producer.h"
#include "util/string_utils.h"
#include "util/system_utils.h"
#include "easylogging++.h"

namespace log2hdfs {

std::unique_ptr<Log2kafkaProduce> Log2kafkaProduce::Init(
    std::shared_ptr<KafkaProducer> producer,
    std::shared_ptr<Queue<std::string>> queue,
    std::shared_ptr<Log2kafkaOffsetTable> table,
    std::shared_ptr<Log2kafkaErrmsgHandle> handle) {
  if (!producer || !queue || !table || !handle)
    return nullptr;

  return std::unique_ptr<Log2kafkaProduce>(new Log2kafkaProduce(
             std::move(producer), std::move(queue), std::move(table),
             std::move(handle)));
}

bool Log2kafkaProduce::AddTopic(std::shared_ptr<Log2kafkaTopicConf> conf) {
  if (!conf)
    return false;

  std::unique_ptr<KafkaTopicConf> topic_conf = conf->kafka_topic_conf();
  const std::string topic_name = conf->topic_name();

  std::string errstr;
  std::shared_ptr<KafkaTopicProducer> ktp = producer_->CreateTopicProducer(
      topic_name, topic_conf.get(), &errstr);
  if (!ktp) {
    LOG(ERROR) << "Log2kafkaProduce AddTopic CreateTopicProducer topic["
               << topic_name << "] failed errstr[" << errstr << "]";
    return false;
  }

  std::lock_guard<std::mutex> guard(mutex_);
  auto it = topic_confs_.find(topic_name);
  if (it != topic_confs_.end()) {
    LOG(WARNING) << "Log2kafkaProduce AddTopic topic[" << topic_name
                 << "] already add";
    return false;
  }

  LOG(INFO) << "Log2kafkaProduce AddTopic topic[" << topic_name
            << "] successed";
  topic_confs_[topic_name] = std::move(conf);
  return true;
}

bool Log2kafkaProduce::RemoveTopic(const std::string& topic) {
  if (topic.empty())
    return false;

  std::lock_guard<std::mutex> guard(mutex_);
  if (topic_confs_.erase(topic) == 1) {
    LOG(INFO) << "Log2kafkaProduce RemoveTopic topic_confs_ erase "
              << "topic[" << topic << "] successed";
  } else {
    LOG(WARNING) << "Log2kafkaProduce RemoveTopic topic_confs_ erase "
                 << "topic[" << topic << "] failed";
  }

  if (producer_->RemoveTopicProducer(topic)) {
    LOG(INFO) << "Log2kafkaProduce RemoveTopic RemoveTopicProducer topic["
              << topic << "] successed";
  } else {
    LOG(WARNING) << "Log2kafkaProduce RemoveTopic RemoveTopicProducer topic["
                 << topic << "] failed";
  }
  return true;
}

void Log2kafkaProduce::Start() {
  std::lock_guard<std::mutex> guard(thread_mutex_);
  if (!thread_.joinable()) {
    running_.store(true);
    std::thread t(&Log2kafkaProduce::StartInternal, this);
    thread_ = std::move(t);
  }
}

void Log2kafkaProduce::Stop() {
  running_.store(false);
  std::lock_guard<std::mutex> guard(thread_mutex_);
  if (thread_.joinable())
    thread_.join();
}

void Log2kafkaProduce::StartInternal() {
  LOG(INFO) << "Log2kafkaProduce thread created";
  while (running_.load()) {
    std::shared_ptr<std::string> value = queue_->WaitPop();

    if (!value) {
      LOG(WARNING) << "Log2kafkaProduce StartInternal WaitPop nullptr path";
      continue;
    }

    std::vector<std::string> vec = SplitString(*value, ":", kTrimWhitespace,
                                               kSplitNonempty);
    if (vec.size() < 3) {
      LOG(WARNING) << "Log2kafkaProduce StartInternal invalid path:"
                   << *value;
      continue;
    }

    const std::string& topic = vec[0];
    const std::string& path = vec[1];
    off_t offset = atol(vec[2].c_str());
    if (topic.empty() || path.empty() || offset < 0) {
      LOG(WARNING) << "Log2kafkaProduce StartInternal invalid path:"
                   << *value;
      continue;
    }

    if (!IsFile(path)) {
      LOG(WARNING) << "Log2kafkaProduce StartInternal IsFile path["
                   << path << "] failed";
      continue;
    }

    std::shared_ptr<KafkaTopicProducer> ktp =
        producer_->GetTopicProducer(topic);
    if (!ktp) {
      LOG(WARNING) << "Log2kafkaProduce StartInternal invalid "
                   << "KafkaTopicProducer topic:" << topic;
      continue;
    }

    std::unique_lock<std::mutex> guard(mutex_);
    auto it = topic_confs_.find(topic);
    if (it == topic_confs_.end()) {
      LOG(WARNING) << "Log2kafkaProduce StartInternal invalid "
                   << "topic_confs topic:" << topic;
      guard.unlock();
      continue;
    }

    std::shared_ptr<Log2kafkaTopicConf> ltc = it->second;
    guard.unlock();

    int batch = ltc->produce_batch_num();
    int timeout = ltc->poll_timeout();
    int msgs_num = ltc->poll_messages();

    ProduceAndSave(topic, path, offset, batch, timeout,
                   msgs_num, std::move(ktp));
  }

  LOG(INFO) << "Log2kafkaProduce thread existing";
}

#define PRODUCE_TRY_NUM 3;

void Log2kafkaProduce::ProduceAndSave(const std::string& topic,
                                      const std::string& path, off_t offset,
                                      int batch, int timeout, int msgs_num,
                                      std::shared_ptr<KafkaTopicProducer> ktp) {
  Optional<std::string> dirname = DirName(path);
  Optional<std::string> basename = BaseName(path);
  if (!dirname.valid() || !basename.valid()) {
    LOG(WARNING) << "Log2kafkaProduce ProduceAndSave invalid path:" << path;
    return;
  }

  table_->Update(dirname.value(), basename.value(), 0);
/*

  std::ifstream ifs(path);
  if (!ifs.is_open()) {
    LOG(WARNING) << "Log2kafkaProduce ProduceAndSave open path["
                 << path << "] failed";
    return;
  }
  ifs.seekg(offset);

  off_t num = 0;
  std::string line;
  while (std::getline(ifs, line)) {
    int i = 0;
    for (; i < 3; ++i) {
      int res = ktp->ProduceMessage(line);
      if (res == 0)
        break;

      if (res == -2) {
        LOG(WARNING) << "Log2kafkaProduce ProduceAndSave path["
                     << path << "] empy line numnber[" << num << "]";
        break;
      }
      
      int en = errno;
      if (en == 105) {
        producer_->PollOutq(msgs_num, timeout);
      } else {
        ktp->Poll(timeout);
      }
    }

    // Try ProduceMessage failed 3 times, handle line with error msg
    if (i >= 3) {
      LOG(WARNING) << "Log2kafkaProduce ProduceAndSave failed 3 times "
                   << "topic[" << topic << "] path[" << path << "]"
                   << " errno[" << errno << "]";
      handle_->ArchiveMsg(topic, line);
    }

    if (++num % batch == 0) {
      if (!table_->Update(dirname.value(), basename.value(), ifs.tellg())) {
        LOG(WARNING) << "Log2kafkaProduce table_ Update failed "
                     << "topic[" << topic << "] path[" << path << "]"
                     << " offset[" << ifs.tellg() << "]";
      }
    }
  }

  if (!table_->Update(dirname.value(), basename.value(), ifs.tellg())) {
    LOG(WARNING) << "Log2kafkaProduce table_ Update failed "
                 << "topic[" << topic << "] path[" << path << "]"
                 << " offset[" << ifs.tellg() << "]";
  }

  ifs.close();
  LOG(INFO) << "log sent[" << path << "] line number[" << num  << "]";

*/
}

}   // namespace log2hdfs
