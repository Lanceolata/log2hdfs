// Copyright (c) 2017 Lanceolata

#include "log2kafka/produce.h"
#include "log2kafka/topic_conf.h"
#include "log2kafka/offset_table.h"
#include "log2kafka/errmsg_handle.h"
#include "kafka/kafka_producer.h"
#include "kafka/kafka_topic_producer.h"
#include "util/string_utils.h"
#include "util/system_utils.h"
#include "easylogging++.h"

namespace log2hdfs {

std::unique_ptr<Produce> Produce::Init(
    std::shared_ptr<KafkaProducer> producer,
    std::shared_ptr<Queue<std::string>> queue,
    std::shared_ptr<OffsetTable> table,
    std::shared_ptr<ErrmsgHandle> handle) {
  if (!producer || !queue || !table || !handle) {
    LOG(ERROR) << "Produce Init invalid parameters";
    return nullptr;
  }
  return std::unique_ptr<Produce>(new Produce(
             std::move(producer), std::move(queue),
             std::move(table), std::move(handle)));
}

bool Produce::AddTopic(std::shared_ptr<TopicConf> conf) {
  if (!conf) {
    LOG(WARNING) << "Produce AddTopic invalid parameters";
    return false;
  }

  std::unique_ptr<KafkaTopicConf> topic_conf = conf->kafka_topic_conf();
  const std::string topic = conf->topic();

  std::string errstr;
  std::shared_ptr<KafkaTopicProducer> ktp = producer_->CreateTopicProducer(
      topic, topic_conf.get(), &errstr);
  if (!ktp) {
    LOG(WARNING) << "Produce AddTopic CreateTopicProducer topic[" << topic
                 << "] failed with errstr[" << errstr << "]";
    return false;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  auto it = topic_confs_.find(topic);
  if (it != topic_confs_.end()) {
    LOG(WARNING) << "Produce AddTopic topic[" << topic
                 << "] already add";
    return false;
  }

  LOG(INFO) << "Produce AddTopic topic[" << topic << "] success";
  topic_confs_[topic] = std::move(conf);
  return true;
}

bool Produce::RemoveTopic(const std::string& topic) {
  if (topic.empty()) {
    LOG(WARNING) << "Produce RemoveTopic invalid parameters";
    return false;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  if (topic_confs_.erase(topic) == 1) {
    LOG(INFO) << "Produce RemoveTopic topic_confs_ erase topic[" << topic
              << "] success";
  } else {
    LOG(WARNING) << "Produce RemoveTopic topic_confs_ erase topic[" << topic
                 << "] failed";
    return false;
  }

  if (producer_->RemoveTopicProducer(topic)) {
    LOG(INFO) << "Produce RemoveTopic RemoveTopicProducer topic[" << topic
              << "] success";
  } else {
    LOG(WARNING) << "Produce RemoveTopic RemoveTopicProducer topic["
                 << topic << "] failed";
    return false;
  }
  return true;
}

void Produce::StartInternal() {
  LOG(INFO) << "Log2kafkaProduce thread created";

  while (!stop_.load()) {
    std::shared_ptr<std::string> record = queue_->WaitPop();
    if (!record || record->empty()) {
      LOG(INFO) << "Produce StartInternal WaitPop null";
      continue;
    }

    // check record
    std::vector<std::string> vec = SplitString(*record, ":",
        kTrimWhitespace, kSplitNonempty);
    if (vec.size() < 3) {
      LOG(WARNING) << "Produce StartInternal invalid record[" << *record<< "]";
      continue;
    }

    const std::string& topic = vec[0];
    const std::string& path = vec[1];
    off_t offset = atol(vec[2].c_str());
    if (topic.empty() || path.empty() || offset < -1) {
      LOG(WARNING) << "Produce StartInternal invalid record topic[" << topic
                   << "] path[" << path << "] offset[" << offset << "]";
      continue;
    }

    if (!IsFile(path)) {
      LOG(WARNING) << "Produce StartInternal IsFile failed topic[" << topic
                   << "] path[" << path << "] offset[" << "offset" << "]";
      continue;
    }

    // get topic producer
    std::shared_ptr<KafkaTopicProducer> ktp =
        producer_->GetTopicProducer(topic);
    if (!ktp) {
      LOG(WARNING) << "Produce StartInternal GetTopicProducer topic["
                   << topic << "] failed";
      continue;
    }

    // get topic conf
    std::unique_lock<std::mutex> lock(mutex_);
    auto it = topic_confs_.find(topic);
    if (it == topic_confs_.end()) {
      LOG(WARNING) << "Produce StartInternal invalid topic_confs topic["
                   << topic << "]";
      lock.unlock();
      continue;
    }

    std::shared_ptr<TopicConf> conf = it->second;
    lock.unlock();

    // get conf parameter
    int batch = conf->batch_num();
    int timeout = conf->poll_timeout();
    int msgs = conf->poll_messages();

    ProduceAndSave(topic, path, offset, batch, timeout, msgs, std::move(ktp));
  }

  LOG(INFO) << "Produce thread existing";
}

#define PRODUCE_TRY_NUM 3

void Produce::ProduceAndSave(
    const std::string& topic,
    const std::string& path,
    off_t offset,
    int batch,
    int timeout,
    int msgs_num,
    std::shared_ptr<KafkaTopicProducer> ktp) {
  std::string dir = DirName(path);
  std::string file = BaseName(path);
  if (dir.empty() || file.empty()) {
    LOG(WARNING) << "Produce ProduceAndSave invlaid path[" << path << "]";
    return;
  }
/*
  table_->Update(dir, file, 0);
  LOG(INFO) << "log sent topic[" << topic << "] path[" << path << "] offset["
            << offset << "] batch[" << batch << "] timeout[" << timeout
            <<"] msgs_num[" << msgs_num << "]";
*/

  std::ifstream ifs(path);
  if (!ifs.is_open()) {
    LOG(WARNING) << "Produce ProduceAndSave open path[" << path << "] failed";
    return;
  }

  if (offset == -1)
    return;

  if (offset != 0)
    ifs.seekg(offset);

  int i = 0;
  off_t num = 0;
  std::string line;
  while (std::getline(ifs, line)) {
    ++num;
    for (i = 0; i < PRODUCE_TRY_NUM; ++i) {
      int res = ktp->ProduceMessage(line);
      if (res == 0)
        break;

      if (res == -2) {
        LOG(WARNING) << "Produce ProduceAndSave path[" << path
                     << "] empty line[" << num << "]";
        break;
      }

      if (errno == 105) {
        producer_->PollOutq(msgs_num, timeout);
      } else {
        ktp->Poll(timeout);
      }
    }

    // Try ProduceMessage failed 3 times, handle line with error msg
    if (i >= 3) {
      LOG(WARNING) << "Produce ProduceAndSave failed 3 times "
                   << "topic[" << topic << "] path[" << path << "]"
                   << " errno[" << errno << "]";
      handle_->ArchiveMsg(topic, line);
    }

    if (num % batch == 0) {
      if (!table_->Update(dir, file, ifs.tellg())) {
        LOG(WARNING) << "Produce ProduceAndSave table Update failed "
                     << "topic[" << topic << "] path[" << path << "]"
                     << " offset[" << ifs.tellg() << "]";
      }
    }
  }

  if (!table_->Update(dir, file, ifs.tellg())) {
    LOG(WARNING) << "Produce ProduceAndSave table Update failed "
                 << "topic[" << topic << "] path[" << path << "]"
                 << " offset[" << ifs.tellg() << "]";
  }
  ifs.close();

  producer_->PollOutq(msgs_num, timeout);
  LOG(INFO) << "log sent[" << path << "] line[" << num << "]";

}

}   // namespace log2hdfs
