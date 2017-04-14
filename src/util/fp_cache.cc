// Copyright (c) 2017 Lanceolata

#include "util/fp_cache.h"
#include <unistd.h>     // include for sleep
#include <utility>      // include for make_pair
#include "util/logger.h"

namespace log2hdfs {

namespace {

void Destructor(FILE *fp) {
  if (fp) {
    if (fclose(fp) != 0) {
      Log(LogLevel::kLogError, "fclose faild with errno[%d]", errno);
    }
  } else {
    Log(LogLevel::kLogWarn, "unexpected null FP");
  }
}

}   // namespace

std::shared_ptr<FILE> FpCache::Get(const std::string &key) {
  return Get(key, key);
}

std::shared_ptr<FILE> FpCache::Get(const std::string &key,
                                   const std::string &path) {
  std::shared_ptr<FILE> res;
  std::unordered_map<std::string, std::shared_ptr<FILE> >::const_iterator it;

  pthread_rwlock_rdlock(&lock_);

  it = cache_.find(key);
  if (it == cache_.end()) {
    pthread_rwlock_unlock(&lock_);
    pthread_rwlock_wrlock(&lock_);

    if ((it = cache_.find(key)) != cache_.end()) {
      res = it->second;
    } else {
      FILE *fp;
      if ((fp = fopen(path.c_str(), "a")) == NULL) {
        Log(LogLevel::kLogWarn, "fopen[%s] failed with errno[%d]",
            path.c_str(), errno);
        res = nullptr;
      } else {
        res.reset(fp, Destructor);
        cache_.insert(std::make_pair(key, res));
      }
    }

  } else {
    res = it->second;
  }

  pthread_rwlock_unlock(&lock_);

  return res;
}

#define ERASE_TIMES 6

FpCache::RemoveResult FpCache::Remove(const std::string &key) {
  std::shared_ptr<FILE> fptr = nullptr;
  std::unordered_map<std::string, std::shared_ptr<FILE> >::const_iterator it;

  pthread_rwlock_wrlock(&lock_);

  it = cache_.find(key);
  if (it == cache_.end()) {
    return FpCache::RemoveResult::kInvalidKey;
  } else {
    fptr = it->second;
    cache_.erase(it);
  }

  pthread_rwlock_unlock(&lock_);

  // 判断指针的引用是否为1(当前函数引用1次) 最多循环6次
  int count = 0;
  while (count < ERASE_TIMES && fptr.use_count() > 1) {
    ++count;
    sleep(1);
  }

  if (fptr.use_count() > 1) {
    return FpCache::RemoveResult::kRemoveFailed;
  }

  return FpCache::RemoveResult::kRemoveOk;
}

void FpCache::Clean() {
  pthread_rwlock_wrlock(&lock_);
  cache_.clear();
  pthread_rwlock_unlock(&lock_);
}

}   // namespace log2hdfs
