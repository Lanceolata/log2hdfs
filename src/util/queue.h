// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_UTIL_QUEUE_H_
#define LOG2HDFS_UTIL_QUEUE_H_

#include <queue>
#include <mutex>
#include <memory>
#include <condition_variable>

namespace log2hdfs {

/**
 * Simple thread safe queue.
 */
template<class T>
class Queue {
 public:
  /**
   * Static function to create a Queue shared_ptr.
   * 
   * @returns std::shared_ptr<Queue>
   */
  static std::shared_ptr<Queue> Init() {
    return std::make_shared<Queue>();
  }

  /**
   * Constructor
   */
  Queue() {}

  Queue(const Queue& other) = delete;
  Queue& operator=(const Queue& other) = delete;

  /**
   * Push T value to queue.
   */
  void Push(T value) {
    std::shared_ptr<T> data(std::make_shared<T>(std::move(value)));
    std::lock_guard<std::mutex> lock(mutex_);
    queue_.push(data);
    cond_.notify_one();
  }

  /**
   * Wait to pop a value from queue.
   * 
   * @param value pop value
   */
  void WaitPop(T* value) {
    if (!value)
      return;

    std::unique_lock<std::mutex> lock(mutex_);
    cond_.wait(lock, [this]{ return !queue_.empty(); });
    *value = std::move(*queue_.front());
    queue_.pop();
  }

  /**
   * Wait to pop a value from queue.
   * 
   * @returns std::shared_ptr<T> point to pop value.
   */
  std::shared_ptr<T> WaitPop() {
    std::unique_lock<std::mutex> lock(mutex_);
    cond_.wait(lock, [this]{ return !queue_.empty(); });
    std::shared_ptr<T> res = queue_.front();
    queue_.pop();
    return res;
  }

  /**
   * Try to pop a value from queue.
   * 
   * @param value pop value
   * 
   * @returns true if pop and set value success; false otherwise.
   */
  bool TryPop(T* value) {
    if (!value)
      return false;

    std::lock_guard<std::mutex> lock(mutex_);
    if (queue_.empty())
      return false;

    *value = std::move(*queue_.front());
    queue_.pop();
    return true;
  }

  /**
   * Try to pop a value from queue.
   * 
   * @returns std::shared_ptr<T> point to pop value if pop success;
   *          nullptr otherwise.
   */
  std::shared_ptr<T> TryPop() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (queue_.empty())
      return nullptr;

    std::shared_ptr<T> res = queue_.front();
    queue_.pop();
    return res;
  }

  /**
   * Whether the queue is empty.
   * 
   * @returns true if queue is empty; false otherwise.
   */
  bool Empty() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.empty();
  }

 private:
  mutable std::mutex mutex_;
  std::condition_variable cond_;
  std::queue<std::shared_ptr<T>> queue_;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_UTIL_QUEUE_H_
