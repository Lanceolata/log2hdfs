// Copyright (c) 2017 Lanceolata

#include "util/fp_cache.h"
#include <unistd.h>     // include for sleep
#include <utility>      // include for make_pair
#include "easylogging++.h"

namespace log2hdfs {

namespace {

void Destructor(FILE* fp) {
  if (fp) {
    if (fclose(fp) != 0)
      LOG(ERROR) << "fclose faild with errno[" << errno << "]";
  } else {
    LOG(WARNING) << "unexpected null FP";
  }
}

}   // namespace

std::shared_ptr<FILE> FpCache::Get(const std::string& key) {
  std::shared_ptr<FILE> res;

  pthread_rwlock_rdlock(&lock_);

  auto it = cache_.find(key);
  if (it != cache_.end()) {
    res = it->second;
  }

  pthread_rwlock_unlock(&lock_);

  return res;
}

std::shared_ptr<FILE> FpCache::Get(const std::string& key,
                                   const std::string& path) {
  std::shared_ptr<FILE> res;

  pthread_rwlock_rdlock(&lock_);

  auto it = cache_.find(key);
  if (it != cache_.end()) {
    res = it->second;
  } else {
    pthread_rwlock_unlock(&lock_);
    pthread_rwlock_wrlock(&lock_);

    if ((it = cache_.find(key)) != cache_.end()) {
      res = it->second;
    } else {
      FILE *fp;
      if ((fp = fopen(path.c_str(), "a")) != NULL) {
        res.reset(fp, Destructor);
        cache_.insert(std::make_pair(key, res));
        paths_.insert(std::make_pair(key, path));
      }
    }
  }

  pthread_rwlock_unlock(&lock_);

  return res;
}

const int erase_times = 6;

FpCache::RemoveResult FpCache::Remove(const std::string& key) {
  std::shared_ptr<FILE> fptr;

  pthread_rwlock_wrlock(&lock_);

  auto it = cache_.find(key);
  if (it != cache_.end()) {
    fptr = it->second;
    cache_.erase(it);

    auto it2 = paths_.find(key);
    if (it2 != paths_.end())
      paths_.erase(it2);
  }

  pthread_rwlock_unlock(&lock_);

  if (!fptr)
    return FpCache::RemoveResult::kInvalidKey;

  int count = 0;
  while (count < erase_times && fptr.use_count() > 1) {
    ++count;
    sleep(1);
  }

  if (fptr.use_count() > 1) {
    return FpCache::RemoveResult::kRemoveFailed;
  } else {
    fptr.reset();
  }

  return FpCache::RemoveResult::kRemoveOk;
}

std::vector<std::string> FpCache::CloseAll() {
  std::vector<std::string> vec;

  pthread_rwlock_wrlock(&lock_);

  cache_.clear();
  for (auto it = paths_.begin(); it != paths_.end(); ++it) {
    vec.push_back(it->second);
  }
  paths_.clear();

  pthread_rwlock_unlock(&lock_);

  return vec;
}

void FpCache::Clear() {
  pthread_rwlock_wrlock(&lock_);

  cache_.clear();
  paths_.clear();

  pthread_rwlock_unlock(&lock_);
}

}   // namespace log2hdfs
