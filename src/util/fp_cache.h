// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_UTIL_FP_CACHE_H_
#define LOG2HDFS_UTIL_FP_CACHE_H_

#include <pthread.h>
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>

namespace log2hdfs {

/**
 * Simple thread safe fp cache.
 */
class FpCache {
 public:
  /**
   * Static function to create a FpCache shared_ptr.
   * 
   * @return std::shared_ptr<FpCache>.
   */
  static std::shared_ptr<FpCache> Init() {
    return std::make_shared<FpCache>();
  }

  /**
   * Constructor.
   * 
   * Init pthread_rwlock_t.
   */
  FpCache() {
    if (pthread_rwlock_init(&lock_, NULL) != 0)
      throw "init pthread_rwlock failed!!!";
  }

  /**
   * Destructor.
   * 
   * Clear all FILE* and paths, then destory pthread_rwlock_t.
   */
  ~FpCache() {
    Clear();
    pthread_rwlock_destroy(&lock_);
  }

  FpCache(const FpCache& other) = delete;
  FpCache& operator=(const FpCache& other) = delete;

  /**
   * Get fp cache from map.
   * 
   * @param key         key to match.
   *
   * @return            std::shared_ptr<FILE> if key was found;
   *                    nullptr otherwise.
   */
  std::shared_ptr<FILE> Get(const std::string& key);

  /**
   * Get fp cache from map.
   * 
   * @param key         key to match.
   * @param path        if key not match, path will open.
   *
   * @return            std::shared_ptr<FILE> key was found or open(path)
   *                    success; nullptr otherwise.
   */
  std::shared_ptr<FILE> Get(const std::string& key, const std::string& path);

  /**
   * FpCache Remove result.
   */
  enum RemoveResult {
    kInvalidKey = -2,   /**< key was not found. */
    kRemoveFailed = -1, /**< remove from map success but fclose failed. */
    kRemoveOk = 0       /**< remove and fclose success. */
  };

  /**
   * Remove fp cache from map.
   * 
   * @param key         key to math.
   *
   * @return            RemoveResult. @see RemoveResult
   */
  FpCache::RemoveResult Remove(const std::string& key);

  /**
   * Erase all fp cache and return cache fp paths.
   * May not thread safe.
   */
  std::vector<std::string> CloseAll();

  /**
   * Clear all fp cache and paths.
   */
  void Clear();

 private:
  mutable pthread_rwlock_t lock_;
  /**< key <--> FILE* */
  std::unordered_map<std::string, std::shared_ptr<FILE>> cache_;
  /**< key <--> path */
  std::unordered_map<std::string, std::string> paths_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_UTIL_FP_CACHE_H_
