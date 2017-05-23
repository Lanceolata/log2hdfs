// Copyright (c) 2017 Lanceolata

#include "log2kafka/topic_conf.h"
#include "util/configparser.h"
#include "util/string_utils.h"
#include "util/system_utils.h"
#include "easylogging++.h"

namespace log2hdfs {

// ------------------------------------------------------------------
// TopicConfContents

TopicConfContents::TopicConfContents():
    kafka_topic_conf_(KafkaTopicConf::Init()),
    remedy_(-1),
    batch_num_(100),
    poll_timeout_(200),
    poll_messages_(2000) {}

TopicConfContents::TopicConfContents(const TopicConfContents& other):
    kafka_topic_conf_(other.kafka_topic_conf_->Copy()),
    remedy_(other.remedy_),
    batch_num_(other.batch_num_.load()),
    poll_timeout_(other.poll_timeout_.load()),
    poll_messages_(other.poll_messages_.load()) {}

// rdkafka conf in section[default] start with "kafka.".
#define KAFKA_PREFIX "kafka."
#define KAFKA_PREFIX_LEN 6

bool TopicConfContents::Update(std::shared_ptr<Section> section) {
  if (!section) {
    LOG(WARNING) << "TopicConfContents Update invalid parameters";
    return false;
  }

  if (!UpdateRuntime(section)) {
    LOG(WARNING) << "TopicConfContents Update UpdateRuntime failed";
    return false;
  }

  Optional<std::string> option = section->Get("remedy");
  if (option.valid() && !option.value().empty()) {
    time_t remedy = atol(option.value().c_str());
    if (remedy < -1) {
      LOG(WARNING) << "TopicConfContents Update invalid remedy["
                   << remedy << "]";
      return false;
    } else {
      remedy_ = remedy;
    }
  }
  LOG(INFO) << "TopicConfContents Update remedy[" << remedy_ << "] success";

  std::string errstr;
  for (auto it = section->Begin(); it != section->End(); ++it) {
    if (StartsWith(it->first, KAFKA_PREFIX)) {
      std::string name = it->first.substr(KAFKA_PREFIX_LEN);
      if (kafka_topic_conf_->Set(name, it->second, &errstr) == kConfOk) {
        LOG(INFO) << "TopicConfContents Update set kafka conf name[" << name
                  << "] value[" << it->second << "] success";
      } else {
        LOG(WARNING) << "TopicConfContents Update set kafka conf name[" << name
                     << "] value[" << it->second << "] failed";
        return false;
      }
    }
  }
  return true;
}

bool TopicConfContents::UpdateRuntime(std::shared_ptr<Section> section) {
  if (!section) {
    LOG(WARNING) << "TopicConfContents UpdateRuntime invalid parameters";
    return false;
  }

  int batch_num = batch_num_.load();
  Optional<std::string> option = section->Get("batch.num");
  if (option.valid() && !option.value().empty()) {
    batch_num = atoi(option.value().c_str());
    if (batch_num <= 0) {
      LOG(WARNING) << "TopicConfContents UpdateRuntime invalid batch_num["
                   << batch_num << "]";
      return false;
    }
  }

  int poll_timeout = poll_timeout_.load();
  option = section->Get("poll.timeout");
  if (option.valid() && !option.value().empty()) {
    poll_timeout = atoi(option.value().c_str());
    if (poll_timeout <= 0) {
      LOG(WARNING) << "TopicConfContents UpdateRuntime invalid poll_timeout["
                   << poll_timeout << "]";
      return false;
    }
  }

  int poll_messages = poll_messages_.load();
  option = section->Get("poll.messages");
  if (option.valid() && !option.value().empty()) {
    poll_messages = atoi(option.value().c_str());
    if (poll_messages <= 0) {
      LOG(WARNING) << "TopicConfContents UpdateRuntime invalid poll_messages["
                   << poll_messages << "]";
      return false;
    }
  }

  if (batch_num != batch_num_.load()) {
    batch_num_.store(batch_num);
    LOG(INFO) << "TopicConfContents UpdateRuntime update batch_num["
              << batch_num << "] success";
  }

  if (poll_timeout != poll_timeout_.load()) {
    poll_timeout_.store(poll_timeout);
    LOG(INFO) << "TopicConfContents UpdateRuntime update poll_timeout["
              << poll_timeout << "] success";
  }

  if (poll_messages != poll_messages_.load()) {
    poll_messages_.store(poll_messages);
    LOG(INFO) << "TopicConfContents UpdateRuntime update poll_messages["
              << poll_messages << "] success";
  }
  return true;
}

// ------------------------------------------------------------------
// TopicConf

// static member variable initialization.
TopicConfContents TopicConf::DEFAULT_CONTENTS_;

bool TopicConf::UpdataDefaultConf(std::shared_ptr<Section> section) {
  LOG(INFO) << "TopicConf UpdataDefaultConf";
  return DEFAULT_CONTENTS_.Update(std::move(section));
}

std::shared_ptr<TopicConf> TopicConf::Init(const std::string& topic) {
  if (topic.empty()) {
    LOG(ERROR) << "TopicConf Init invalid parameters";
    return nullptr;
  }
  return std::make_shared<TopicConf>(topic);
}

bool TopicConf::InitConf(std::shared_ptr<Section> section) {
  LOG(INFO) << "TopicConf InitConf topic[" << topic_ << "]";
  if (!section) {
    LOG(WARNING) << "TopicConf InitConf invalid parameters";
    return false;
  }

  Optional<std::string> option = section->Get("dirs");
  if (!option.valid() || option.value().empty()) {
    LOG(WARNING) << "TopicConf InitConf invalid dirs";
    return false;
  }

  std::vector<std::string> dirs = SplitString(option.value(), ",",
      kTrimWhitespace, kSplitNonempty);
  if (dirs.size() == 0) {
    LOG(WARNING) << "TopicConf InitConf invalid dirs[" << option.value()
                 << "]";
    return false;
  }

  std::string dirpaths;
  for (auto& dir : dirs) {
    if (!IsDir(dir)) {
      LOG(WARNING) << "TopicConf InitConf invalud dir[" << dir << "]";
      return false;
    }
    dirpaths.append(dir);
    dirpaths.append(",");

    dirs_.push_back(dir);
  }
  LOG(INFO) << "TopicConf InitConf update dirs[" << dirpaths << "] success";
  return contents_.Update(std::move(section));
}

bool TopicConf::UpdateRuntime(std::shared_ptr<Section> section) {
  LOG(INFO) << "TopicConf UpdateRuntime topic[" << topic_ << "]";
  if (!section) {
    LOG(WARNING) << "TopicConf UpdateRuntime invalid parameters";
    return false;
  }
  return contents_.UpdateRuntime(std::move(section));
}

}   // namespace log2hdfs
