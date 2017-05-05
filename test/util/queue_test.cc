// Copyright (c) 2017 Lanceolata

#include "gtest/gtest.h"
#include "util/queue.h"
#include <string>
#include <thread>
#include "easylogging++.h"

using namespace log2hdfs;

namespace {

TEST(Queue, PushTryPop) {
  std::shared_ptr<Queue<std::string>> queue = Queue<std::string>::Init();
  queue->Push("queue_push_1");
  queue->Push("queue_push_2");
  queue->Push("queue_push_3");
  std::shared_ptr<std::string> res = queue->TryPop();
  ASSERT_EQ("queue_push_1", *res);
  std::string value;
  queue->TryPop(&value);
  ASSERT_EQ("queue_push_2", value);
  res = queue->TryPop();
  ASSERT_EQ("queue_push_3", *res);
  res = queue->TryPop();
  ASSERT_EQ(nullptr, res);
}

void PushWaitPopTest1(std::shared_ptr<Queue<std::string>> queue) {
  for (int i = 0; i < 3; ++i) {
    std::shared_ptr<std::string> value = queue->WaitPop();
    LOG(INFO) << "PushWaitPopTest1:" << *value;
    sleep(2);
  }
}

void PushWaitPopTest2(std::shared_ptr<Queue<std::string>> queue) {
  sleep(1);
  std::string value;
  for (int i = 0; i < 3; ++i) {
    queue->WaitPop(&value);
    LOG(INFO) << "PushWaitPopTest2:" << value;
    sleep(2);
  }
}

TEST(Queue, PushWaitPop) {
  std::shared_ptr<Queue<std::string>> queue = Queue<std::string>::Init();
  std::thread t1(PushWaitPopTest1, queue);
  std::thread t2(PushWaitPopTest2, queue);
  sleep(3);
  queue->Push("PushWaitPop test1");
  queue->Push("PushWaitPop test2");
  queue->Push("PushWaitPop test3");
  queue->Push("PushWaitPop test4");
  queue->Push("PushWaitPop test5");
  queue->Push("PushWaitPop test6");
  t1.join();
  t2.join();
  ASSERT_TRUE(queue->Empty());
}

TEST(Queue, Empty) {
  std::shared_ptr<Queue<std::string>> queue = Queue<std::string>::Init();
  ASSERT_TRUE(queue->Empty());
  queue->Push("queue_push_1");
  ASSERT_FALSE(queue->Empty());
}

}
