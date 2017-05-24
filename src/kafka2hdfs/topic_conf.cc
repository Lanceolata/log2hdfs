// Copyright (c) 2017 Lanceolata

#include "kafka2hdfs/topic_conf.h"
#include <stdlib.h>
#include "util/configparser.h"
#include "util/string_utils.h"
#include "util/system_utils.h"
#include "easylogging++.h"

namespace log2hdfs {

namespace {

bool ParsePartitions(
    const std::string& input,
    const std::vector<std::string>& topics,
    std::vector<std::vector<int32_t>>* partitions) {
  std::vector<std::string> vec1 = SplitString(input, ";",
      kTrimWhitespace, kSplitNonempty);
  if (vec1.size() != topics.size())
    return false;

  for (auto& t : vec1) {
    std::vector<int32_t> tps;
    std::vector<std::string> vec2 = SplitString(t, ",",
        kTrimWhitespace, kSplitNonempty);
    if (vec2.size() == 0) {
      return false;
    }

    for (auto& p : vec2) {
      std::size_t found = p.find("-");
      if (found == std::string::npos) {
        int32_t partition = atoi(p.c_str());
        if (partition < 0) {
          return false;
        }
        tps.push_back(partition);
      } else {
        int32_t start = atoi(p.substr(0, found).c_str());
        int32_t end = atoi(p.substr(found + 1).c_str());
        if (start < 0 || end < 0 || end <= start) {
          return false;
        }

        for (int32_t i = start; i <= end; ++i) {
          tps.push_back(i);
        }
      }
    }

    partitions->push_back(std::move(tps));
  }

  return true;
}

bool ParseOffsets(
    const std::string& input,
    const std::vector<std::vector<int32_t>>& partitions,
    std::vector<std::vector<int64_t>>* offsets) {
  std::vector<std::string> vec1 = SplitString(input, ";",
      kTrimWhitespace, kSplitNonempty);
  if (vec1.size() != partitions.size())
    return false;

  for (size_t i = 0; i < vec1.size(); ++i) {
    std::vector<int64_t> tos;
    std::vector<std::string> vec2 = SplitString(vec1[i], ",",
        kTrimWhitespace, kSplitNonempty);
    if (vec2.size() == 0) {
      return false;
    }

    int64_t last;
    for (auto& o : vec2) {
      last = atol(o.c_str());
      tos.push_back(last);
    }

    for (size_t j = tos.size(); j < partitions[i].size(); ++j) {
      tos.push_back(last);
    }

    offsets->push_back(std::move(tos));
  }
  return true;
}

}   // namespace

// ------------------------------------------------------------------
// TopicConfContents

TopicConfContents::TopicConfContents():
    consume_dir_(""),
    compress_dir_(""),
    upload_dir_(""),
    kafka_topic_conf_(KafkaTopicConf::Init()),
    log_format_(LogFormat::Type::kV6),
    path_format_(PathFormat::Type::kNormal),
    consume_type_(ConsumeCallback::Type::kV6),
    file_format_(Upload::Type::kText),
    parallel_(3),
    compress_lzo_(),
    compress_orc_(),
    compress_mv_(),
    consume_interval_(900),
    complete_interval_(120),
    complete_maxsize_(21474836480),
    complete_maxseconds_(-1),
    upload_interval_(20) {}

TopicConfContents::TopicConfContents(const TopicConfContents& other):
    consume_dir_(other.consume_dir_),
    compress_dir_(other.compress_dir_),
    upload_dir_(other.upload_dir_),
    kafka_topic_conf_(other.kafka_topic_conf_->Copy()),
    log_format_(other.log_format_),
    path_format_(other.path_format_),
    consume_type_(other.consume_type_),
    file_format_(other.file_format_),
    parallel_(other.parallel_),
    compress_lzo_(other.compress_lzo_),
    compress_orc_(other.compress_orc_),
    compress_mv_(other.compress_mv_),
    consume_interval_(other.consume_interval_.load()),
    complete_interval_(other.complete_interval_.load()),
    complete_maxsize_(other.complete_maxsize_.load()),
    complete_maxseconds_(other.complete_maxseconds_.load()),
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

  Optional<std::string> option = section->Get("rootdir");
  if (option.valid()) {
    std::string normal_path = NormalDirPath(option.value());
    if (normal_path.empty()) {
      LOG(WARNING) << "TopicConfContents Update invalid normal_path";
      return false;
    }
    consume_dir_ = normal_path + "/consume";
    compress_dir_ = normal_path + "/compress";
    upload_dir_ = normal_path + "/upload";
  }
  LOG(INFO) << "TopicConfContents Update consume_dir_[" << consume_dir_
            << "] compress_dir_[" << compress_dir_ << "] upload_dir_["
            << upload_dir_ << "]";

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

  option = section->Get("file.format");
  if (option.valid()) {
    Optional<Upload::Type> file_format = Upload::ParseType(option.value());
    if (file_format.valid()) {
      file_format_ = file_format.value();
    } else {
      LOG(WARNING) << "TopicConfContents Update invalid file_format["
                   << option.value() << "]";
      return false;
    }
  }
  LOG(INFO) << "TopicConfContents Update file_format["
            << file_format_ << "]";

  option = section->Get("parallel");
  if (option.valid()) {
    long parallel = atol(option.value().c_str());
    if (parallel < 0) {
      LOG(WARNING) << "TopicConfContents Update invalid parallel["
                   << option.value() << "]";
      return false;
    } else {
      parallel_ = parallel;
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
    if (complete_maxsize < 0) {
      LOG(WARNING) << "TopicConfContents UpdateRuntime invalid "
                   << "complete_maxsize[" << complete_maxsize << "]";
      return false;
    }
  }

  int complete_maxseconds = complete_maxseconds_.load();
  option = section->Get("complete.maxseconds");
  if (option.valid() && !option.value().empty()) {
    complete_maxseconds = atoi(option.value().c_str());
  }

  int upload_interval = upload_interval_.load();
  option = section->Get("upload.interval");
  if (option.valid() && !option.value().empty()) {
    upload_interval = atoi(option.value().c_str());
    if (upload_interval < 0) {
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

  if (complete_maxseconds != complete_maxseconds_.load()) {
    complete_maxseconds_.store(complete_maxseconds);
    LOG(INFO) << "TopicConfContents UpdateRuntime update complete_maxseconds["
              << complete_maxseconds << "] success";
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

std::shared_ptr<TopicConf> TopicConf::Init(const std::string& section) {
  if (section.empty()) {
    LOG(ERROR) << "TopicConf Init invalid parameters";
    return nullptr;
  }
  return std::make_shared<TopicConf>(section);
}

bool TopicConf::InitConf(std::shared_ptr<Section> section) {
  LOG(INFO) << "TopicConf InitConf section[" << section_ << "]";
  if (!section) {
    LOG(WARNING) << "TopicConf InitConf invalid parameters";
    return false;
  }

  std::string topics = section->Get("topics", "");
  if (topics.empty()) {
    LOG(WARNING) << "TopicConf InitConf invalid topics";
    return false;
  }

  topics_ = SplitString(topics, ";", kTrimWhitespace, kSplitNonempty);
  if (topics_.size() == 0) {
    LOG(WARNING) << "TopicConf InitConf invalid topics[" <<  topics << "]";
    return false;
  }

  std::string partitions = section->Get("partitions", "");
  if (partitions.empty()) {
    LOG(WARNING) << "TopicConf InitConf invalid partitions";
    return false;
  }

  if (!ParsePartitions(partitions, topics_, &partitions_)) {
    LOG(WARNING) << "TopicConf InitConf ParsePartitions[" << partitions
                 << "] failed";
    return false;
  }

  std::string offsets = section->Get("offsets", "");
  if (offsets.empty()) {
    LOG(WARNING) << "TopicConf InitConf invalid offsets";
    return false;
  }
  
  if (!ParseOffsets(offsets, partitions_, &offsets_)) {
    LOG(WARNING) << "TopicConf InitConf ParseOffsets[" << offsets
                 << "] failed";
    return false;
  }

  for (size_t i = 0; i < topics_.size(); ++i) {
    const std::string& top = topics_[i];
    const std::vector<int32_t>& pars = partitions_[i];
    const std::vector<int64_t>& offs = offsets_[i];
    std::stringstream stream;
    for (size_t j = 0; j < pars.size(); ++j) {
      stream << pars[j] << ":" << offs[j] << ",";
    }
    LOG(INFO) << "TopicConf InitConf topic[" << top << "] partitions["
              << stream.str() << "] success";
  }

  std::string hdfs_path = section->Get("hdfs.path", "");
  if (hdfs_path.empty()) {
    LOG(WARNING) << "TopicConf InitConf hdfs_path invalid";
    return false;
  }
  hdfs_path_ = hdfs_path;
  LOG(INFO) << "TopicConf InitConf hdfs_path[" << hdfs_path << "]";

  return contents_.Update(std::move(section));
}

bool TopicConf::UpdateRuntime(std::shared_ptr<Section> section) {
  LOG(INFO) << "TopicConf UpdateRuntime section[" << section_ << "]";
  if (!section) {
    LOG(WARNING) << "TopicConf UpdateRuntime invalid parameters";
    return false;
  }
  return contents_.UpdateRuntime(std::move(section));
}

}   // namespace log2hdfs
