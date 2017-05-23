// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_UTIL_THREAD_POOL_H_
#define LOG2HDFS_UTIL_THREAD_POOL_H_

#include <vector>
#include <queue>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>
#include <functional>
#include <stdexcept>

class ThreadPool {
 public:
  ThreadPool(size_t threads);

  template<class F, class... Args>
  auto Enqueue(F&& f, Args&&... args)
      -> std::future<typename std::result_of<F(Args...)>::type>;

  ~ThreadPool();

 private:
  // need to keep track of threads so we can join them
  std::vector<std::thread> workers_;

  // the task queue
  std::queue< std::function<void()> > tasks_;

  // synchronization
  std::mutex mutex_;
  std::condition_variable cond_;
  bool stop_;
};

// the constructor just launches some amount of workers
inline ThreadPool::ThreadPool(size_t threads):
    stop_(false) {
  for (size_t i = 0; i < threads; ++i) {
    workers_.emplace_back(
      [this]
      {
        for(;;) {
          std::function<void()> task;
          {
            std::unique_lock<std::mutex> lock(this->mutex_);
            this->cond_.wait(lock,
                [this]{ return this->stop_ || !tasks_.empty(); });
            if (this->stop_)
              return;

            task = std::move(this->tasks_.front());
            this->tasks_.pop();
          }

          task();
        }
      }
    );
  }
}

// add new work item to the pool
template<class F, class... Args>
auto ThreadPool::Enqueue(F&& f, Args&&... args)
    -> std::future<typename std::result_of<F(Args...)>::type> {
  using return_type = typename std::result_of<F(Args...)>::type;

  auto task = std::make_shared<std::packaged_task<return_type()>>(
      std::bind(std::forward<F>(f), std::forward<Args>(args)...));

  std::future<return_type> res = task->get_future();
  {
    std::unique_lock<std::mutex> lock(mutex_);
    tasks_.emplace([task](){ (*task)(); });
  }
  cond_.notify_one();
  return res;
}

// the destructor joins all threads
inline ThreadPool::~ThreadPool() {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    stop_ = true;
  }
  cond_.notify_all();
  for (auto& worker : workers_) {
    worker.join();
  }
}

#endif  // LOG2HDFS_UTIL_THREAD_POOL_H_
