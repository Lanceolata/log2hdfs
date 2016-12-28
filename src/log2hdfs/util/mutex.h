#ifndef LOG2HDFS_UTIL_MUTEX_H
#define LOG2HDFS_UTIL_MUTEX_H

#include <errno.h>
#include <pthread.h>

namespace log2hdfs {

namespace util {

class Mutex {
 public:

  Mutex() {
    if ((pthread_mutex_init(&m_lock, NULL) != 0)) {
      throw "init spin lock failed!!!";
    }
  }  

  ~Mutex() {
    pthread_mutex_destroy(&m_lock);
  }

  void lock() {
    pthread_mutex_lock(&m_lock);
  }

  void unlock() {
    pthread_mutex_unlock(&m_lock);
  }

  pthread_mutex_t& mutex() {
    return m_lock;
  }

 private:

  pthread_mutex_t m_lock;
};

}   // namespace util

}   // namespace log2hdfs

#endif  // LOG2HDFS_UTIL_MUTEX_H
