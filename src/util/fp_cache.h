// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_UTIL_FP_CACHE_H_
#define LOG2HDFS_UTIL_FP_CACHE_H_

#include <stdio.h>
#include <pthread.h>
#include <string>
#include <memory>
#include <unordered_map>

namespace log2hdfs {

class FpCache {
 public:
  static std::shared_ptr<FpCache> Init() {
    return std::make_shared<FpCache>();
  }

  FpCache() {
    if (pthread_rwlock_init(&lock_, NULL) != 0) {
      throw "init pthread_rwlock failed!!!";
    }
  }

  FpCache(const FpCache &other) {
    pthread_rwlock_wrlock(&(other.lock_));
    cache_ = other.cache_;
    pthread_rwlock_unlock(&(other.lock_));
  }

  FpCache &operator=(const FpCache &other) {
    if (this != &other) {
      pthread_rwlock_wrlock(&(other.lock_));
      cache_ = other.cache_;
      pthread_rwlock_unlock(&(other.lock_));
    }
    return *this;
  }

  ~FpCache() {
    Clean();
    pthread_rwlock_destroy(&lock_);
  }

  std::shared_ptr<FILE> Get(const std::string &key);

  std::shared_ptr<FILE> Get(const std::string &key, const std::string &path);

  bool Remove(const std::string &key);

  void Clean();

 private:
  mutable pthread_rwlock_t lock_;
  std::unordered_map<std::string, std::shared_ptr<FILE> > cache_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_UTIL_FP_CACHE_H_
