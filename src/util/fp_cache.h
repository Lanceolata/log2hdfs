// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_UTIL_FP_CACHE_H_
#define LOG2HDFS_UTIL_FP_CACHE_H_

#include <stdio.h>
#include <pthread.h>
#include <string>
#include <memory>
#include <unordered_map>

namespace log2hdfs {

/*
 * FpCache
 * 文件指针缓存
 * 内部使用linux读写锁
 * 
 * */
class FpCache {
 public:
  enum RemoveResult {
    kInvalidKey = -2,
    kRemoveFailed = -1,
    kRemoveOk = 0
  };

  // 初始化
  static std::shared_ptr<FpCache> Init() {
    return std::make_shared<FpCache>();
  }

  // Constructor
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

  // 获得缓存的文件指针
  // 传入key，内部调用Get(key, key)
  std::shared_ptr<FILE> Get(const std::string &key);

  // 根据key获得文件指针，如不存在，则打开path，并插入到map中
  std::shared_ptr<FILE> Get(const std::string &key, const std::string &path);

  // 移除文件指针
  // Return:
  //  - kInvalidKey     传入的key不存在 
  //  - kRemoveFailed   移除失败(超过最大等待次数)
  //  - kRemoveOk       移除成功
  FpCache::RemoveResult Remove(const std::string &key);

  // 清空缓存
  void Clean();

 private:
  // 读写锁
  mutable pthread_rwlock_t lock_;
  // 缓存map
  std::unordered_map<std::string, std::shared_ptr<FILE> > cache_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_UTIL_FP_CACHE_H_
