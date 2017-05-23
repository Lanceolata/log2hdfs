// Copyright (c) 2017 Lanceolata

#include "kafka2hdfs/consume_callback.h"
#include "kafka2hdfs/path_format.h"
#include "kafka2hdfs/topic_conf.h"
#include "util/system_utils.h"
#include "easylogging++.h"

namespace log2hdfs {

// ------------------------------------------------------------------
// ConsumeCallback

Optional<ConsumeCallback::Type> ConsumeCallback::ParseType(
    const std::string& type) {
  if (type == "v6") {
    return Optional<ConsumeCallback::Type>(kV6);
  } else if (type == "ef") {
    return Optional<ConsumeCallback::Type>(kEf);
  } else if (type == "report") {
    return Optional<ConsumeCallback::Type>(kReport);
  } else if (type == "debug") {
    return Optional<ConsumeCallback::Type>(kDebug);
  } else {
    return Optional<ConsumeCallback::Type>::Invalid();
  }
}

std::shared_ptr<KafkaConsumeCb> ConsumeCallback::Init(
    std::shared_ptr<TopicConf> conf,
    std::shared_ptr<PathFormat> format,
    std::shared_ptr<FpCache> cache) {
  if (!conf || !format || !cache) {
    LOG(ERROR) << "ConsumeCallback Init invalid parameters";
    return nullptr;
  }

  std::shared_ptr<KafkaConsumeCb> res;
  ConsumeCallback::Type type = conf->consume_type();
  switch (type) {
    case kV6:
      res = V6ConsumeCallback::Init(std::move(conf), std::move(format),
                                    std::move(cache));
      break;
    case kEf:
      res = EfConsumeCallback::Init(std::move(conf), std::move(format),
                                    std::move(cache));
      break;
    case kReport:
      res = ReportConsumeCallback::Init(std::move(conf), std::move(format),
                                        std::move(cache));
      break;
    case kDebug:
      res = DebugConsumeCallback::Init(std::move(conf), std::move(format),
                                       std::move(cache));
      break;
    default:
      LOG(ERROR) << "ConsumeCallback Init unknown type";
      res.reset();
  }
  return res;
}

// ------------------------------------------------------------------
// V6ConsumeCallback

std::shared_ptr<V6ConsumeCallback> V6ConsumeCallback::Init(
    std::shared_ptr<TopicConf> conf,
    std::shared_ptr<PathFormat> format,
    std::shared_ptr<FpCache> cache) {
  if (!conf || !format || !cache) {
    LOG(ERROR) << "V6ConsumeCallback Init invalid parameters";
    return nullptr;
  }

  std::string consume_dir = conf->consume_dir();
  std::string normal_path = NormalDirPath(consume_dir);
  if (!IsDir(normal_path)) {
    if (!MakeDir(normal_path)) {
      LOG(ERROR) << "V6ConsumeCallback Init MakeDir[" << normal_path
                 << "] failed";
      return nullptr;
    }
  }

  return std::make_shared<V6ConsumeCallback>(normal_path,
             std::move(format), std::move(cache));
}

void V6ConsumeCallback::Consume(const KafkaMessage& msg) {
  char *payload = static_cast<char *>(msg.Payload());
  size_t len = msg.Len();

  std::string filename;
  if (!format_->BuildLocalFileName(msg, &filename)) {
    LOG(WARNING) << "V6ConsumeCallback Consume BuildLocalFileName topic["
                 << msg.TopicName() << "] msg[" << payload << "] failed";
    return;
  }

  std::shared_ptr<FILE> fptr = cache_->Get(filename);
  if (!fptr) {
    std::string path = dir_ + "/" + filename + "." + std::to_string(time(NULL));
    fptr = cache_->Get(filename, path);
    if (!fptr) {
      LOG(ERROR) << "V6ConsumeCallback Consume Get fptr filename[" << filename
                 << "] path[" << path << "] failed";
      return;
    }
  }

  std::unique_ptr<char[]> temp(new char[len + 1]);
  char *p = temp.get();
  memcpy(p, payload, len);
  temp[len] = '\n';

  size_t n = fwrite(p, 1, len + 1, fptr.get());
  if (n != len + 1) {
    LOG(ERROR) << "V6ConsumeCallback Consume fwrite[" << filename
               << "] might fail written[" << n << "] expected["
               << len + 1;
  }
}

// ------------------------------------------------------------------
// ReportConsumeCallback

std::shared_ptr<ReportConsumeCallback> ReportConsumeCallback::Init(
    std::shared_ptr<TopicConf> conf,
    std::shared_ptr<PathFormat> format,
    std::shared_ptr<FpCache> cache) {
  if (!conf || !format || !cache) {
    LOG(ERROR) << "ReportConsumeCallback Init invalid parameters";
    return nullptr;
  }

  std::string consume_dir = conf->consume_dir();
  if (!IsDir(consume_dir)) {
    if (!MakeDir(consume_dir)) {
      LOG(ERROR) << "ReportConsumeCallback Init MakeDir[" << consume_dir
                 << "] failed";
      return nullptr;
    }
  }

  return std::make_shared<ReportConsumeCallback>(consume_dir,
             std::move(format), std::move(cache));
}

void ReportConsumeCallback::Consume(const KafkaMessage& msg) {
  char *payload = static_cast<char *>(msg.Payload());
  size_t len = msg.Len();

  std::string filename;
  if (!format_->BuildLocalFileName(msg, &filename)) {
    LOG(WARNING) << "ReportConsumeCallback Consume BuildLocalFileName topic["
                 << msg.TopicName() << "] msg[" << payload << "] failed";
    return;
  }

  std::shared_ptr<FILE> fptr = cache_->Get(filename);
  if (!fptr) {
    std::string path = dir_ + "/" + filename + "." + std::to_string(time(NULL));
    fptr = cache_->Get(filename, path);
    if (!fptr) {
      LOG(ERROR) << "ReportConsumeCallback Consume Get fptr filename["
                 << filename << "] path[" << path << "] failed";
      return;
    }
  }

  char *pt = strchr(payload, '\t') + 1;
  len = len - (pt - payload);
  payload = pt;

  std::unique_ptr<char[]> temp(new char[len + 1]);
  char *p = temp.get();
  memcpy(p, payload, len);
  temp[len] = '\n';

  size_t n = fwrite(p, 1, len + 1, fptr.get());
  if (n != len + 1) {
    LOG(ERROR) << "ReportConsumeCallback Consume fwrite[" << filename
               << "] might fail written[" << n << "] expected["
               << len + 1;
  }
}

// ------------------------------------------------------------------
// DebugConsumeCallback

std::shared_ptr<DebugConsumeCallback> DebugConsumeCallback::Init(
    std::shared_ptr<TopicConf> conf,
    std::shared_ptr<PathFormat> format,
    std::shared_ptr<FpCache> cache) {
  if (!conf || !format || !cache) {
    LOG(ERROR) << "DebugConsumeCallback Init invalid parameters";
    return nullptr;
  }

  std::string consume_dir = conf->consume_dir();
  if (!IsDir(consume_dir)) {
    if (!MakeDir(consume_dir)) {
      LOG(ERROR) << "DebugConsumeCallback Init MakeDir[" << consume_dir
                 << "] failed";
      return nullptr;
    }
  }

  return std::make_shared<DebugConsumeCallback>(consume_dir,
             std::move(format), std::move(cache));
}

void DebugConsumeCallback::Consume(const KafkaMessage& msg) {
  char *payload = static_cast<char *>(msg.Payload());
  size_t len = msg.Len();

  std::string filename;
  if (!format_->BuildLocalFileName(msg, &filename)) {
    LOG(WARNING) << "V6ConsumeCallback Consume BuildLocalFileName topic["
                 << msg.TopicName() << "] msg[" << payload << "] failed";
    return;
  }

  std::shared_ptr<FILE> fptr = cache_->Get(filename);
  if (!fptr) {
    std::string path = dir_ + "/" + filename + "." + std::to_string(time(NULL));
    fptr = cache_->Get(filename, path);
    if (!fptr) {
      LOG(ERROR) << "V6ConsumeCallback Consume Get fptr filename[" << filename
                 << "] path[" << path << "] failed";
      return;
    }
  }

  std::string data = msg.TopicName() + ":" + std::to_string(msg.Offset())
      + ":" + std::string(payload, len) + "\n";
  len =  data.size();
  char* p = const_cast<char *>(data.c_str());

  size_t n = fwrite(p, 1, len + 1, fptr.get());
  if (n != len + 1) {
    LOG(ERROR) << "V6ConsumeCallback Consume fwrite[" << filename
               << "] might fail written[" << n << "] expected["
               << len + 1;
  }
}

}   // namespace log2hdfs
