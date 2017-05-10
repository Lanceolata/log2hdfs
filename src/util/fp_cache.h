// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_UTIL_FP_CACHE_H_
#define LOG2HDFS_UTIL_FP_CACHE_H_

#include <pthread.h>
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>

namespace log2hdfs {

// Simple thread safe fp cache.
class FpCache {
 public:
  static std::shared_ptr<FpCache> Init() {
    return std::make_shared<FpCache>();
  }

  FpCache() {
    // Init pthread_rwlock_t.
    if (pthread_rwlock_init(&lock_, NULL) != 0)
      throw "init pthread_rwlock failed!!!";
  }

  ~FpCache() {
    Clear();
    pthread_rwlock_destroy(&lock_);
  }

  FpCache(const FpCache& other) = delete;
  FpCache& operator=(const FpCache& other) = delete;

  // Get fp cache from map.
  // param:
  //  - key                     key to match
  //
  // return:
  //  - std::shared_ptr<FILE>   key was found
  //  - nullptr                 key was not found
  std::shared_ptr<FILE> Get(const std::string& key);

  // Get fp cache from map.
  // param:
  //  - key                     key to match
  //  - path                    if key not match, path will open
  //
  // return:
  //  - std::shared_ptr<FILE>   key was found or open(path) successed
  //  - nullptr                 key was not found and open(path) failed
  std::shared_ptr<FILE> Get(const std::string& key, const std::string& path);

  enum RemoveResult {
    kInvalidKey = -2,
    kRemoveFailed = -1,
    kRemoveOk = 0
  };

  // Remove fp cache from map.
  // param:
  //  - key                     key to math
  //
  // return:
  //  - kInvalidKey             key was not found
  //  - kRemoveFailed           remove from map successed but fclose failed
  //  - kRemoveOk               remove successed
  FpCache::RemoveResult Remove(const std::string& key);

  // Erase all fp cache and return cache fp paths.
  // May not thread safe.
  std::vector<std::string> CloseAll();

  void Clear();

 private:
  mutable pthread_rwlock_t lock_;
  // key <--> FILE*
  std::unordered_map<std::string, std::shared_ptr<FILE>> cache_;
  // key <--> path
  std::unordered_map<std::string, std::string> paths_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_UTIL_FP_CACHE_H_
