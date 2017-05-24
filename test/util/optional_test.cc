// Copyright (c) 2017 Lanceolata

#include "gtest/gtest.h"
#include "util/optional.h"


namespace {

TEST(Optional, Invalid) {
  Optional<int> testInvalid;
  ASSERT_FALSE(testInvalid.valid());
  ASSERT_EQ(Optional<int>::Invalid(), testInvalid);
}

TEST(Optional, Valid) {
  Optional<int> testValid(3);
  ASSERT_TRUE(testValid.valid());
  ASSERT_EQ(3, testValid.value());
  ASSERT_NE(Optional<int>::Invalid(), testValid);
}

TEST(Optional, Optional) {
  Optional<int> testValid(3);
  Optional<int> testInvalid;

  Optional<int> testCopy = testInvalid;
  ASSERT_FALSE(testCopy.valid());
  ASSERT_EQ(testInvalid, testCopy);

  testCopy = testValid;
  ASSERT_TRUE(testCopy.valid());
  ASSERT_EQ(3, testCopy.value());
  ASSERT_EQ(testValid, testCopy);

  testCopy = std::move(testValid);
  ASSERT_TRUE(testCopy.valid());
  ASSERT_EQ(3, testCopy.value());
  ASSERT_FALSE(testValid.valid());
}

}   // namespace
