// Copyright (c) 2017 Lanceolata

#include "gtest/gtest.h"
#include "util/fp_cache.h"
#include "easylogging++.h"

using namespace log2hdfs;

namespace {


TEST(FpCache, Get) {
  std::shared_ptr<FpCache> fc = FpCache::Init();
  EXPECT_EQ(nullptr, fc->Get("get"));
  EXPECT_EQ(nullptr, fc->Get(""));
}

TEST(FpCache, GetPath) {
  std::shared_ptr<FpCache> fc = FpCache::Init();
  EXPECT_EQ(nullptr, fc->Get("get"));
  std::shared_ptr<FILE> fptr = fc->Get("test", "/tmp/fp_cache_test");
  EXPECT_NE(nullptr, fptr);
  size_t n = fwrite("test\n", 1, 5, fptr.get());
  EXPECT_EQ(5, n);
  fptr.reset();
  fc->Clear();
}

TEST(FpCache, Remove) {
  std::shared_ptr<FpCache> fc = FpCache::Init();
  EXPECT_EQ(FpCache::RemoveResult::kInvalidKey, fc->Remove("remove"));
  std::shared_ptr<FILE> fptr = fc->Get("remove", "/tmp/fp_cache_test");
  EXPECT_NE(nullptr, fptr);
  EXPECT_EQ(FpCache::RemoveResult::kRemoveFailed, fc->Remove("remove"));
  fptr.reset();

  fptr = fc->Get("remove", "/tmp/fp_cache_test");
  fptr.reset();
  EXPECT_EQ(FpCache::RemoveResult::kRemoveOk, fc->Remove("remove"));
  fc->Clear();
}

TEST(FpCache, CloseAll) {
  std::shared_ptr<FpCache> fc = FpCache::Init();
  std::shared_ptr<FILE> fptr = fc->Get("remove", "/tmp/fp_cache_test");
  fptr = fc->Get("closeall", "/tmp/closeall");
  fptr.reset();
  std::vector<std::string> vec = fc->CloseAll();
  ASSERT_EQ(2, vec.size());
  for (auto& path : vec) {
    LOG(INFO) << "CloseAll:" << path;
  }

  fptr = fc->Get("remove", "/tmp/fp_cache_test");
  fptr = fc->Get("closeall", "/tmp/closeall");
  fptr.reset();

  fc->Remove("closeall");
  vec = fc->CloseAll();
  ASSERT_EQ(1, vec.size());
  for (auto& path : vec) {
    LOG(INFO) << "CloseAll:" << path;
  }
}

TEST(FpCache, Clear) {
  std::shared_ptr<FpCache> fc = FpCache::Init();
  std::shared_ptr<FILE> fptr = fc->Get("clear", "/tmp/fp_cache_test_clear");
  EXPECT_NE(nullptr, fptr);
  fc->Clear();
  EXPECT_EQ(nullptr, fc->Get("clear"));
  fptr.reset();
}

}
