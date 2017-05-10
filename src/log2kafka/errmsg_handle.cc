// Copyright (c) 2017 Lanceolata

#include "log2kafka/errmsg_handle.h"
#include <unistd.h>
#include <vector>
#include "util/configparser.h"
#include "util/fp_cache.h"
#include "util/queue.h"
#include "util/system_utils.h"
#include "easylogging++.h"

namespace log2hdfs {

#define DEFAULT_HANDLE_DIR "remedy"
#define DEFAULT_HANDLE_INTERVAL "1800"
#define DEFAULT_HANDLE_REMEDY ""

std::shared_ptr<ErrmsgHandle> ErrmsgHandle::Init(
    std::shared_ptr<Section> section,
    std::shared_ptr<Queue<std::string>> queue) {
  if (!section || !queue) {
    LOG(ERROR) << "ErrmsgHandle Init invalid parameters";
    return nullptr;
  }

  std::string dir = section->Get("handle.dir", DEFAULT_HANDLE_DIR);
  dir = NormalDirPath(dir);
  if (dir.empty()) {
    LOG(ERROR) << "ErrmsgHandle Init empty dirpath";
    return nullptr;
  }

  if (!IsDir(dir)) {
    if (!MakeDir(dir)) {
      LOG(ERROR) << "ErrmsgHandle Init MakeDir path[" << dir
                 << "] failed with errno[" << errno << "]";
      return nullptr;
    }
  }

  std::string interval_str = section->Get("handle.interval",
                                          DEFAULT_HANDLE_INTERVAL);
  int interval = atoi(interval_str.c_str());
  if (interval <= 0) {
    LOG(ERROR) << "ErrmsgHandle Init invalid interval[" << interval << "]";
    return nullptr;
  }

  std::string remedy_str = section->Get("handle.remedy",
                                        DEFAULT_HANDLE_REMEDY);
  bool remedy = false;
  if (remedy_str == "true") {
    remedy = true;
  }

  std::shared_ptr<FpCache> cache = FpCache::Init();
  if (!cache) {
    LOG(ERROR) << "ErrmsgHandle Init FpCache failed";
    return nullptr;
  }

  LOG(INFO) << "ErrmsgHandle Init parameters dir[" << dir
            << "] interval[" << interval << "] remedy[" << remedy
            << "]";
  return std::make_shared<ErrmsgHandle>(dir, interval, remedy,
             std::move(cache), std::move(queue));
}

void ErrmsgHandle::ArchiveMsg(const std::string& topic,
                              const std::string& msg) {
  std::shared_ptr<FILE> fptr = cache_->Get(topic);
  if (!fptr) {
    std::string path = dir_ + "/" + topic + "." +
        std::to_string(time(NULL));
    fptr = cache_->Get(topic, path);
    if (!fptr) {
      LOG(ERROR) << "ErrmsgHandle ArchiveMsg cache_ get topic["
                 << topic << "] failed msg[" << msg << "]";
      return;
    }
  }

  std::string temp = msg + "\n";
  size_t n = fwrite(temp.c_str(), 1, temp.length(), fptr.get());
  if (n != temp.length()) {
    LOG(ERROR) << "ErrmsgHandle ArchiveMsg fwrite topic[" << topic
               << "] msg[" << msg << "] might failed written[" << n
               << "] expected[" << temp.length() << "]";
  }
}

void ErrmsgHandle::StartInternal() {
  LOG(INFO) << "ErrmsgHandle thread created";

  while (true) {
    sleep(interval_);

    std::vector<std::string> vec = cache_->CloseAll();
    // wait for all cache fp close.
    sleep(5);
    for (auto& path : vec) {
      LOG(INFO) << "ErrmsgHandle StartInternal handle path[" << path << "]";

      if (!remedy_)
        continue;

      std::string basename = BaseName(path);
      if (basename.empty()) {
        LOG(WARNING) << "ErrmsgHandle StartInternal BaseName[" << path
                     << "] failed";
        continue;
      }
      auto end = basename.find(".");
      if (end == std::string::npos) {
        LOG(WARNING) << "ErrmsgHandle StartInternal invalid path[" << path
                     << "]";
        continue;
      }

      std::string topic = basename.substr(0, end);
      queue_->Push(topic + ":" + path + ":0");
    }
  }
}

}   // namespace log2hdfs
