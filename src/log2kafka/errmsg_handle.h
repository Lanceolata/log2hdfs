// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_LOG2KAFKA_ERRMSG_HANDLE_H_
#define LOG2HDFS_LOG2KAFKA_ERRMSG_HANDLE_H_

#include <string>
#include <memory>
#include <mutex>
#include <thread>

#ifdef __cplusplus
extern "C" {
#endif
#include "librdkafka/rdkafka.h"
#ifdef __cplusplus
}
#endif

#include "util/queue.h"

namespace log2hdfs {

class Section;
class FpCache;

class ErrmsgHandle {
 public:
  static std::shared_ptr<ErrmsgHandle> Init(
      std::shared_ptr<Section> section,
      std::shared_ptr<Queue<std::string>> queue);

  ErrmsgHandle(const std::string& dir,
               int interval,
               bool remedy,
               std::shared_ptr<FpCache> cache,
               std::shared_ptr<Queue<std::string>> queue):
      dir_(dir), interval_(interval), remedy_(remedy),
      cache_(std::move(cache)), queue_(std::move(queue)) {}

  ErrmsgHandle(const ErrmsgHandle& other) = delete;
  ErrmsgHandle& operator=(const ErrmsgHandle& other) = delete;

  void ArchiveMsg(const std::string& topic, const std::string& msg);

  void Start() {
    std::call_once(flag_, &ErrmsgHandle::CreateThread, this);
  }

 private:
  void CreateThread() {
    std::thread t(&ErrmsgHandle::StartInternal, this);
    t.detach();
  }

  void StartInternal();

  std::string dir_;
  int interval_;
  bool remedy_;
  std::shared_ptr<FpCache> cache_;
  std::shared_ptr<Queue<std::string>> queue_;
  std::once_flag flag_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_LOG2KAFKA_ERRMSG_HANDLE_H_
