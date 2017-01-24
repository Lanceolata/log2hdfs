// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_UTIL_MUTEX_H
#define LOG2HDFS_UTIL_MUTEX_H

#include <errno.h>
#include <pthread.h>

namespace log2hdfs {

namespace util {

class Mutex {
 public:
  Mutex() {
    if ((pthread_mutex_init(&mutex_, NULL) != 0)) {
      throw "init spin lock failed!!!";
    }
  }

  Mutex(const Mutex& mutex) = delete;
  Mutex& operator=(const Mutex& mutex) = delete;

  ~Mutex() {
    pthread_mutex_destroy(&mutex_);
  }

  void Lock() {
    pthread_mutex_lock(&mutex_);
  }

  void Unlock() {
    pthread_mutex_unlock(&mutex_);
  }

  pthread_mutex_t& mutex() {
    return mutex_;
  }

 private:
  pthread_mutex_t mutex_;
};

}   // namespace util

}   // namespace log2hdfs

#endif  // LOG2HDFS_UTIL_MUTEX_H
