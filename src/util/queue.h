// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_UTIL_QUEUE_H_
#define LOG2HDFS_UTIL_QUEUE_H_

#include <queue>
#include <memory>
#include <mutex>
#include <condition_variable>

namespace log2hdfs {

// Thread safe queue.
template<typename T>
class Queue {
 public:
  static std::shared_ptr<Queue> Init() {
    return std::make_shared<Queue>();
  }

  Queue() {}

  Queue(const Queue& other) = delete;
  Queue& operator=(const Queue& other) = delete;

  void Push(T value) {
    std::shared_ptr<T> data(std::make_shared<T>(std::move(value)));
    std::lock_guard<std::mutex> lg(mutex_);
    queue_.push(data);
    cond_.notify_one();
  }

  // Wait until pop value.
  void WaitPop(T* value) {
    if (!value)
      return;

    std::unique_lock<std::mutex> lg(mutex_);
    cond_.wait(lg, [this]{ return !queue_.empty(); });
    *value = std::move(*queue_.front());
    queue_.pop();
  }

  // Wait until pop value.
  std::shared_ptr<T> WaitPop() {
    std::unique_lock<std::mutex> lg(mutex_);
    cond_.wait(lg, [this]{ return !queue_.empty(); });
    std::shared_ptr<T> res = queue_.front();
    queue_.pop();
    return res;
  }

  bool TryPop(T* value) {
    if (!value)
      return false;

    std::lock_guard<std::mutex> lg(mutex_);
    if (queue_.empty())
      return false;

    *value = std::move(*queue_.front());
    queue_.pop();
    return true;
  }

  std::shared_ptr<T> TryPop() {
    std::lock_guard<std::mutex> lg(mutex_);
    if (queue_.empty())
      return nullptr;

    std::shared_ptr<T> res = queue_.front();
    queue_.pop();
    return res;
  }

  bool Empty() const {
    std::lock_guard<std::mutex> lg(mutex_);
    return queue_.empty();
  }

 private:
  std::queue<std::shared_ptr<T>> queue_;
  mutable std::mutex mutex_;
  std::condition_variable cond_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_UTIL_QUEUE_H_
