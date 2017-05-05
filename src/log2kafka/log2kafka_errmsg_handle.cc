// Copyright (c) 2017 Lanceolata

#include "log2kafka/log2kafka_errmsg_handle.h"
#include <unistd.h>
#include <vector>
#include "util/fp_cache.h"
#include "util/queue.h"
#include "util/system_utils.h"
#include "easylogging++.h"

namespace log2hdfs {

std::shared_ptr<Log2kafkaErrmsgHandle> Log2kafkaErrmsgHandle::Init(
    const std::string& dirpath, int interval,
    std::shared_ptr<Queue<std::string>> queue) {
  if (dirpath.empty() || interval <= 0 || !queue) {
    LOG(ERROR) << "Log2kafkaErrmsgHandle Init invalid parameters";
    return nullptr;
  }

  if (!IsDir(dirpath)) {
    if (!MakeDir(dirpath)) {
      LOG(ERROR) << "Log2kafkaErrmsgHandle Init MakeDir path["
                 << dirpath << "] failed";
      return nullptr;
    }
  }

  std::shared_ptr<FpCache> cache = FpCache::Init();
  if (!cache) {
     LOG(ERROR) << "Log2kafkaErrmsgHandle Init FpCache Init failed";
     return nullptr;
  }

  std::string normal_path = NormalDirPath(dirpath);
  return std::make_shared<Log2kafkaErrmsgHandle>(normal_path, interval,
             std::move(cache), std::move(queue));
}

void Log2kafkaErrmsgHandle::ArchiveMsg(const std::string& topic,
                                       const std::string& msg) {
  std::shared_ptr<FILE> fptr = cache_->Get(topic);
  if (!fptr) {
    std::string path = dirpath_ + "/" + topic + "." +
        std::to_string(time(NULL));
    fptr = cache_->Get(topic, path);
    if (!fptr) {
      LOG(ERROR) << "Log2kafkaErrmsgHand ArchiveMsg cache_ get "
                 << "topic[" << topic << "] msg[" << msg << "] failed";
      return;
    }
  }

  std::string temp = msg + "\n";
  size_t n = fwrite(temp.c_str(), 1, temp.length(), fptr.get());
  if (n != temp.length()) {
    LOG(ERROR) << "Log2kafkaErrmsgHand ArchiveMsg fwrite "
               << "topic[" << topic << "] msg[" << msg << "] "
               << "might failed written:" << n << " expected:"
               << temp.length();
  }
}

void Log2kafkaErrmsgHandle::StartInternal() {
  LOG(INFO) << "Log2kafkaErrmsgHandle thread created";

  while (true) {
    sleep(interval_);

    std::vector<std::string> vec = cache_->CloseAll();
    for (auto& path : vec) {
      LOG(WARNING) << "Log2kafkaErrmsgHandle StartInternal handle path:"
                   << path;
      Optional<std::string> basename = BaseName(path);
      if (!basename.valid()) {
        LOG(WARNING) << "Log2kafkaErrmsgHandle StartInternal BaseName:"
                     << path << " failed";
        continue;
      }

      auto end = basename.value().find(".");
      if (end == std::string::npos) {
        LOG(WARNING) << "Log2kafkaErrmsgHandle StartInternal invalid path:"
                     << path;
        continue;
      }

      std::string topic = basename.value().substr(0, end);
      queue_->Push(topic + ":" + path + ":0");
    }
  }
}

}   // namespace log2hdfs
