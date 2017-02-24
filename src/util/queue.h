// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_UTIL_QUEUE_H_
#define LOG2HDFS_UTIL_QUEUE_H_

#include <queue>
#include <memory>
#include <mutex>
#include <condition_variable>

namespace log2hdfs {

template<typename T>
class Queue {
 public:
  Queue() {}

  Queue(const Queue &q) {
    std::lock_guard<std::mutex> lg(q.mutex_);
    queue_ = q.queue_;
  }

  Queue &operator=(const Queue &q) {
    if (this != &q) {
      std::lock_guard<std::mutex> lg(q.mutex_);
      queue_ = q.queue_;
    }
    return *this;
  }

  void Push(T value) {
    std::lock_guard<std::mutex> lg(mutex_);
    queue_.push(value);
    cond_.notify_one();
  }

  void WaitPop(T *value) {
    if (!value) {
      return;
    }
    std::unique_lock<std::mutex> lg(mutex_);
    cond_.wait(lg, [this]{ return !queue_.empty(); });
    *value = queue_.front();
    queue_.pop();
  }

  std::shared_ptr<T> WaitPop() {
    std::unique_lock<std::mutex> lg(mutex_);
    cond_.wait(lg, [this]{ return !queue_.empty(); });
    std::shared_ptr<T> res(std::make_shared<T>(queue_.front()));
    queue_.pop();
    return res;
  }

  bool TryPop(T *value) {
    if (!value) {
      return false;
    }
    std::lock_guard<std::mutex> lg(mutex_);
    if (queue_.empty()) {
      return false;
    }
    *value = queue_.front();
    queue_.pop();
    return true;
  }

  std::shared_ptr<T> TryPop() {
    std::lock_guard<std::mutex> lg(mutex_);
    if (queue_.empty()) {
      return false;
    }
    std::shared_ptr<T> res(std::make_shared<T>(queue_.font()));
    queue_.pop();
    return res;
  }

  bool empty() const {
    std::lock_guard<std::mutex> lg(mutex_);
    return queue_.empty();
  }

 private:
  mutable std::mutex mutex_;
  std::queue<T> queue_;
  std::condition_variable cond_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_UTIL_QUEUE_H_
