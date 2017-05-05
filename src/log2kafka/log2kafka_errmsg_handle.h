// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_LOG2KAFKA_LOG2KAFKA_ERRMSG_HANDLE_H_
#define LOG2HDFS_LOG2KAFKA_LOG2KAFKA_ERRMSG_HANDLE_H_

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

class FpCache;

class Log2kafkaErrmsgHandle {
 public:
  static std::shared_ptr<Log2kafkaErrmsgHandle> Init(
      const std::string& dirpath, int interval,
      std::shared_ptr<Queue<std::string>> queue);

  Log2kafkaErrmsgHandle(const std::string& dirpath,
                        int interval,
                        std::shared_ptr<FpCache> cache,
                        std::shared_ptr<Queue<std::string>> queue):
      dirpath_(dirpath), interval_(interval), cache_(std::move(cache)),
      queue_(std::move(queue)) {}

  void ArchiveMsg(const std::string& topic, const std::string& msg);

  void Start() {
    std::call_once(flag_, &Log2kafkaErrmsgHandle::CreateThread, this);
  }

 private:
  void CreateThread() {
    std::thread t(&Log2kafkaErrmsgHandle::StartInternal, this);
    t.detach();
  }

  void StartInternal();

  std::string dirpath_;
  int interval_;
  std::shared_ptr<FpCache> cache_;
  std::shared_ptr<Queue<std::string>> queue_;
  std::once_flag flag_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_LOG2KAFKA_LOG2KAFKA_ERRMSG_HANDLE_H_
