// Copyright (c) 2017 Lanceolata

#include "log2kafka/log2kafka_topic_conf.h"
#include <stdlib.h>
#include "util/configparser.h"
#include "util/string_utils.h"
#include "util/system_utils.h"
#include "easylogging++.h"

namespace log2hdfs {

// ------------------------------------------------------------------
// L2KTopicConfContents

LkTopicConfContents::LkTopicConfContents():
    kafka_topic_conf_(KafkaTopicConf::Init(KafkaTopicConf::Type::kProducer)),
    produce_batch_num_(100),
    poll_timeout_(200),
    poll_messages_(2000),
    remedy_expire_(-1) {}

LkTopicConfContents::LkTopicConfContents(const LkTopicConfContents& other):
    kafka_topic_conf_(other.kafka_topic_conf_->Copy()),
    produce_batch_num_(other.produce_batch_num_.load()),
    poll_timeout_(other.poll_timeout_.load()),
    poll_messages_(other.poll_messages_.load()),
    remedy_expire_(other.remedy_expire_) {}

#define KAFKA_PREFIX "kafka."
#define KAFKA_PREFIX_LEN 6

bool LkTopicConfContents::Update(std::shared_ptr<Section> section) {
  if (!section)
    return true;

  bool res = UpdateRuntime(section);
  if (!res)
    return false;

  Optional<std::string> value = section->Get("remedy.expire");
  if (value.valid()) {
    remedy_expire_ = atol(value.value().c_str());
  }
  LOG(INFO) << "LkTopicConfContents Update remedy_expire_:"
            << remedy_expire_;

  std::string errstr;
  Section::const_iterator it = section->Begin();
  for (; it != section->End(); ++it) {
    if (StartsWith(it->first, KAFKA_PREFIX)) {
      std::string key = it->first.substr(KAFKA_PREFIX_LEN);
      if (kafka_topic_conf_->Set(key, it->second, &errstr) != kConfOk) {
        LOG(WARNING) << "LKTopicConfContents Update failed"
                     << " key:" << key << " value:"
                     << it->second << " errstr:" << errstr;
      } else {
        LOG(INFO) << "LkTopicConfContents Update kafka conf"
                  << " key:" << it->first
                  << " value:" << it->second;
      }
    }
  }

  return true;
}

bool LkTopicConfContents::UpdateRuntime(std::shared_ptr<Section> section) {
  if (!section)
    return false;

  int produce_batch_num = produce_batch_num_.load();
  int poll_timeout = poll_timeout_.load();
  int poll_messages = poll_messages_.load();

  Optional<std::string> value = section->Get("produce.batch.num");
  if (value.valid()) {
    produce_batch_num = atoi(value.value().c_str());
    if (produce_batch_num <= 0) {
      LOG(WARNING) << "L2KTopicConfContents UpdateRuntime failed "
                   << "produce.batch.num:" << produce_batch_num;
      return false;
    }
  }

  value = section->Get("poll.timeout");
  if (value.valid()) {
    poll_timeout = atoi(value.value().c_str());
    if (poll_timeout <= 0) {
      LOG(WARNING) << "L2KTopicConfContents UpdateRuntime failed "
                   << "poll.timeout:" << poll_timeout;
      return false;
    }
  }

  value = section->Get("poll.messages");
  if (value.valid()) {
    poll_messages = atoi(value.value().c_str());
    if (poll_messages < 0) {
      LOG(WARNING) << "L2KTopicConfContents UpdateRuntime failed "
                   << "poll.messages:" << poll_messages;
      return false;
    }
  }

  produce_batch_num_.store(produce_batch_num);
  poll_timeout_.store(poll_timeout);
  poll_messages_.store(poll_messages);
  LOG(INFO) << "LkTopicConfContents UpdateRuntime produce_batch_num_:"
            << produce_batch_num_.load();
  LOG(INFO) << "LkTopicConfContents UpdateRuntime poll_timeout_:"
            << poll_timeout_.load();
  LOG(INFO) << "LkTopicConfContents UpdateRuntime poll_messages_:"
            << poll_messages_.load();

  return true;
}

// ------------------------------------------------------------------
// Log2kafkaTopicConf

LkTopicConfContents Log2kafkaTopicConf::DEFAULT_CONTENTS_;

bool Log2kafkaTopicConf::UpdataDefaultConf(std::shared_ptr<Section> section) {
  return DEFAULT_CONTENTS_.Update(std::move(section));
}

std::shared_ptr<Log2kafkaTopicConf> Log2kafkaTopicConf::Init(
    const std::string& topic_name) {
  if (topic_name.empty())
    return nullptr;

  return std::make_shared<Log2kafkaTopicConf>(topic_name);
}

bool Log2kafkaTopicConf::InitConf(std::shared_ptr<Section> section) {
  if (!section)
    return false;

  Optional<std::string> value = section->Get("dirpath");
  if (!value.valid() || value.value().empty())
    return false;

  std::vector<std::string> paths = SplitString(value.value(), ",",
      kTrimWhitespace, kSplitNonempty);
  if (paths.size() == 0)
    return false;

  std::string dirpaths;
  for (auto& path : paths) {
    if (!IsDir(path)) {
      LOG(WARNING) << "InitConf topic:" << topic_name_
                   << " invalid dirpath:" << path;
      return false;
    }
    dirpaths.append(path);
    dirpaths.append(",");

    dir_paths_.push_back(path);
  }

  LOG(INFO) << "InitConf topic:" << topic_name_
            << " dirpaths:" << dirpaths;

  return contents_.Update(std::move(section));
}

bool Log2kafkaTopicConf::UpdateRuntime(std::shared_ptr<Section> section) {
  if (!section)
    return false;

  return contents_.UpdateRuntime(std::move(section));
}

}   // namespace log2hdfs
