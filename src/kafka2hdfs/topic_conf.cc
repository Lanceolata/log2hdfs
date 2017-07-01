// Copyright (c) 2017 Lanceolata

#include "kafka2hdfs/topic_conf.h"
#include <stdlib.h>
#include "util/configparser.h"
#include "util/string_utils.h"
#include "util/system_utils.h"
#include "easylogging++.h"

namespace log2hdfs {

namespace {

bool ParsePartitions(const std::string& input,
                     std::vector<int32_t>* partitions) {
  if (input.empty() || !partitions)
    return false;

  std::vector<std::string> vec = SplitString(input, ",",
      kTrimWhitespace, kSplitNonempty);
  for (auto& p : vec) {
    std::size_t found = p.find("-");
    if (found == std::string::npos) {
      int32_t partition = atoi(p.c_str());
      if (partition < 0)
        return false;

      partitions->push_back(partition);
    } else {
      int32_t start = atoi(p.substr(0, found).c_str());
      int32_t end = atoi(p.substr(found + 1).c_str());
      if (start < 0 || end < 0 || end < start)
        return false;

      for (int32_t i = start; i <= end; ++i) {
        partitions->push_back(i);
      }
    }
  }
  return true;
}

bool ParseOffsets(const std::string& input,
                  size_t length,
                  std::vector<int64_t>* offsets) {
  if (input.empty() || length <= 0 || !offsets)
    return false;

  std::vector<std::string> vec = SplitString(input, ",",
      kTrimWhitespace, kSplitNonempty);
  if (vec.size() > length)
    return false;

  int64_t offset;
  for (auto& o : vec) {
    offset = atol(o.c_str());
    offsets->push_back(offset);
  }

  for (size_t i = vec.size(); i < length; ++i) {
    offsets->push_back(offset);
  }
  return true;
}

}   // namespace

// ------------------------------------------------------------------
// TopicConfContents

TopicConfContents::TopicConfContents():
    root_dir_("."),
    kafka_topic_conf_(KafkaTopicConf::Init()),
    log_format_(LogFormat::Type::kV6),
    path_format_(PathFormat::Type::kNormal),
    consume_type_(ConsumeCallback::Type::kV6),
    upload_type_(Upload::Type::kText),
    parallel_(1),
    compress_lzo_(),
    compress_orc_(),
    compress_mv_(),
    consume_interval_(900),
    complete_interval_(120),
    complete_maxsize_(21474836480),
    retention_seconds_(0),
    upload_interval_(20) {}

TopicConfContents::TopicConfContents(const TopicConfContents& other):
    root_dir_(other.root_dir_),
    kafka_topic_conf_(other.kafka_topic_conf_->Copy()),
    log_format_(other.log_format_),
    path_format_(other.path_format_),
    consume_type_(other.consume_type_),
    upload_type_(other.upload_type_),
    parallel_(other.parallel_),
    compress_lzo_(other.compress_lzo_),
    compress_orc_(other.compress_orc_),
    compress_mv_(other.compress_mv_),
    consume_interval_(other.consume_interval_.load()),
    complete_interval_(other.complete_interval_.load()),
    complete_maxsize_(other.complete_maxsize_.load()),
    retention_seconds_(other.retention_seconds_.load()),
    upload_interval_(other.upload_interval_.load()) {}

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

  Optional<std::string> option = section->Get("root.dir");
  if (option.valid()) {
    std::string normal_path = NormalDirPath(option.value());
    if (normal_path.empty()) {
      LOG(WARNING) << "TopicConfContents Update invalid normal_path";
      return false;
    }
    root_dir_ = normal_path;
  }
  LOG(INFO) << "TopicConfContents Update root_dir_[" << root_dir_ << "]";

  option = section->Get("log.format");
  if (option.valid()) {
    Optional<LogFormat::Type> log_format =
        LogFormat::ParseType(option.value());
    if (log_format.valid()) {
      log_format_ = log_format.value();
    } else {
      LOG(WARNING) << "TopicConfContents Update invalid log_format["
                   << option.value() << "]";
      return false;
    }
  }
  LOG(INFO) << "TopicConfContents Update log_format[" << log_format_ << "]";

  option = section->Get("path.format");
  if (option.valid()) {
    Optional<PathFormat::Type> path_format =
        PathFormat::ParseType(option.value());
    if (path_format.valid()) {
      path_format_ = path_format.value();
    } else {
      LOG(WARNING) << "TopicConfContents Update invalid path_format["
                   << option.value() << "]";
      return false;
    }
  }
  LOG(INFO) << "TopicConfContents Update path_format[" << path_format_ << "]";

  option = section->Get("consume.type");
  if (option.valid()) {
    Optional<ConsumeCallback::Type> consume_type =
        ConsumeCallback::ParseType(option.value());
    if (consume_type.valid()) {
      consume_type_ = consume_type.value();
    } else {
      LOG(WARNING) << "TopicConfContents Update invalid consume_type["
                   << option.value() << "]";
      return false;
    }
  }
  LOG(INFO) << "TopicConfContents Update consume_type["
            << consume_type_ << "]";

  option = section->Get("upload.type");
  if (option.valid()) {
    Optional<Upload::Type> upload_type = Upload::ParseType(option.value());
    if (upload_type.valid()) {
      upload_type_ = upload_type.value();
    } else {
      LOG(WARNING) << "TopicConfContents Update invalid upload_type["
                   << option.value() << "]";
      return false;
    }
  }
  LOG(INFO) << "TopicConfContents Update upload_type["
            << upload_type_ << "]";

  option = section->Get("parallel");
  if (option.valid()) {
    long parallel = atol(option.value().c_str());
    if (parallel > 0 && parallel < 24) {
      parallel_ = parallel;
    } else {
      LOG(WARNING) << "TopicConfContents Update invalid parallel["
                   << option.value() << "]";
      return false;
    }
  }
  LOG(INFO) << "TopicConfContents Update parallel[" << parallel_ << "]";

  option = section->Get("compress.lzo");
  if (option.valid()) {
    compress_lzo_ = option.value();
  }
  LOG(INFO) << "TopicConfContents Update compress_lzo["
            << compress_lzo_ << "]";

  option = section->Get("compress.orc");
  if (option.valid()) {
    compress_orc_ = option.value();
  }
  LOG(INFO) << "TopicConfContents Update compress_orc[" << compress_orc_
            << "]";

  option = section->Get("compress.mv");
  if (option.valid()) {
    compress_mv_ = option.value();
  }
  LOG(INFO) << "TopicConfContents Update compress_mv[" << compress_mv_
            << "]";

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

      if (name == "offset.store.path") {
        if (!MakeDir(it->second)) {
          LOG(WARNING) << "TopicConfContents Update MakeDir[" << it->second
                       << "] failed with errno[" << errno << "]";
          return false;
        }
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

  int consume_interval = consume_interval_.load();
  Optional<std::string> option = section->Get("consume.interval");
  if (option.valid() && !option.value().empty()) {
    consume_interval = atoi(option.value().c_str());
    if (consume_interval < 60) {
      LOG(WARNING) << "TopicConfContents UpdateRuntime invalid "
                   << "consume_interval[" << consume_interval << "]";
      return false;
    }
  }

  int complete_interval = complete_interval_.load();
  option = section->Get("complete.interval");
  if (option.valid() && !option.value().empty()) {
    complete_interval = atoi(option.value().c_str());
    if (complete_interval < 60) {
      LOG(WARNING) << "TopicConfContents UpdateRuntime invalid "
                   << "complete_interval[" << complete_interval << "]";
      return false;
    }
  }

  long complete_maxsize = complete_maxsize_.load();
  option = section->Get("complete.maxsize");
  if (option.valid() && !option.value().empty()) {
    complete_maxsize = atol(option.value().c_str());
  }

  int retention_seconds = retention_seconds_.load();
  option = section->Get("retention.seconds");
  if (option.valid() && !option.value().empty()) {
    retention_seconds = atoi(option.value().c_str());
  }

  int upload_interval = upload_interval_.load();
  option = section->Get("upload.interval");
  if (option.valid() && !option.value().empty()) {
    upload_interval = atoi(option.value().c_str());
    if (upload_interval <= 0) {
      LOG(WARNING) << "TopicConfContents UpdateRuntime invalid "
                   << "upload_interval[" << upload_interval << "]";
      return false;
    }
  }

  if (consume_interval != consume_interval_.load()) {
    consume_interval_.store(consume_interval);
    LOG(INFO) << "TopicConfContents UpdateRuntime update consume_interval["
              << consume_interval << "] success";
  }

  if (complete_interval != complete_interval_.load()) {
    complete_interval_.store(complete_interval);
    LOG(INFO) << "TopicConfContents UpdateRuntime update complete_interval["
              << complete_interval << "] success";
  }

  if (complete_maxsize != complete_maxsize_.load()) {
    complete_maxsize_.store(complete_maxsize);
    LOG(INFO) << "TopicConfContents UpdateRuntime update complete_maxsize["
              << complete_maxsize << "] success";
  }

  if (retention_seconds != retention_seconds_.load()) {
    retention_seconds_.store(retention_seconds);
    LOG(INFO) << "TopicConfContents UpdateRuntime update retention_seconds["
              << retention_seconds << "] success";
  }

  if (upload_interval != upload_interval_.load()) {
    upload_interval_.store(upload_interval);
    LOG(INFO) << "TopicConfContents UpdateRuntime update upload_interval["
              << upload_interval << "] success";
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

  std::string partitions = section->Get("partitions", "");
  if (partitions.empty()) {
    LOG(WARNING) << "TopicConf InitConf invalid partitions";
    return false;
  }

  if (!ParsePartitions(partitions, &partitions_)) {
    LOG(WARNING) << "TopicConf InitConf ParsePartitions[" << partitions
                 << "] failed";
    return false;
  }

  std::string offsets = section->Get("offsets", "");
  if (offsets.empty()) {
    LOG(WARNING) << "TopicConf InitConf invalid offsets";
    return false;
  }

  if (!ParseOffsets(offsets, partitions_.size(), &offsets_)) {
    LOG(WARNING) << "TopicConf InitConf ParseOffsets[" << offsets
                 << "] failed";
    return false;
  }

  std::stringstream stream;
  for (size_t i = 0; i < partitions_.size(); ++i) {
    stream << partitions_[i] << ":" << offsets_[i] << ",";
  }
  LOG(INFO) << "TopicConf InitConf partitions[" << stream.str()
            << "] success";

  std::string hdfs_path = section->Get("hdfs.path", "");
  if (hdfs_path.empty()) {
    LOG(WARNING) << "TopicConf InitConf hdfs_path invalid";
    return false;
  }
  hdfs_path_ = hdfs_path;
  LOG(INFO) << "TopicConf InitConf hdfs_path[" << hdfs_path << "]";

  if (!contents_.Update(std::move(section))) {
    return false;
  }

  // create dirs
  if (!MakeDir(contents_.root_dir_)) {
    LOG(WARNING) << "TopicConf InitConf MakeDir[" << contents_.root_dir_
                 << "] failed with errno[" << errno << "]";
    return false;
  }

  std::string topic_dir = contents_.root_dir_ + "/" + topic_;
  if (!MakeDir(topic_dir)) {
    LOG(WARNING) << "TopicConf InitConf MakeDir[" << topic_dir
                 << "] failed with errno[" << errno << "]";
    return false;
  }
  consume_dir_ = topic_dir + "/" + "consume";
  compress_dir_ = topic_dir + "/" + "compress";
  upload_dir_ = topic_dir + "/" + "upload";
  return true;
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
