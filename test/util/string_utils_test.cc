// Copyright (c) 2017 Lanceolata

#include "gtest/gtest.h"
#include "util/string_utils.h"


namespace {

TEST(StringUtils, SplitString) {
  std::vector<std::string> r;

  r = SplitString(std::string(), ",:;", kKeepWhitespace, kSplitAll);
  EXPECT_TRUE(r.empty());

  // Empty separator list
  r = SplitString("hello, world", "", kKeepWhitespace, kSplitAll);
  ASSERT_EQ(1u, r.size());
  EXPECT_EQ("hello, world", r[0]);

  // Should split on any of the separators.
  r = SplitString("::,,;;", ",:;", kKeepWhitespace, kSplitAll);
  ASSERT_EQ(7u, r.size());
  for (auto str : r)
    ASSERT_TRUE(str.empty());

  r = SplitString("red, green; blue:", ",:;", kTrimWhitespace, kSplitNonempty);
  ASSERT_EQ(3u, r.size());
  EXPECT_EQ("red", r[0]);
  EXPECT_EQ("green", r[1]);
  EXPECT_EQ("blue", r[2]);

  // Want to split a string along whitespace sequences.
  r = SplitString("  red green   \tblue\n", " \t\n", kTrimWhitespace,
                  kSplitNonempty);
  ASSERT_EQ(3u, r.size());
  EXPECT_EQ("red", r[0]);
  EXPECT_EQ("green", r[1]);
  EXPECT_EQ("blue", r[2]);

  // Weird case of splitting on spaces but not trimming.
  r = SplitString(" red ", " ", kTrimWhitespace, kSplitAll);
  ASSERT_EQ(3u, r.size());
  EXPECT_EQ("", r[0]);  // Before the first space.
  EXPECT_EQ("red", r[1]);
  EXPECT_EQ("", r[2]);  // After the last space.
}

// Check different whitespace and result types for SplitString
TEST(StringUtils, SplitString_WhitespaceAndResultType) {
  std::vector<std::string> r;

  // Empty input handling.
  r = SplitString(std::string(), ",", kKeepWhitespace, kSplitAll);
  EXPECT_TRUE(r.empty());
  r = SplitString(std::string(), ",", kKeepWhitespace, kSplitNonempty);
  EXPECT_TRUE(r.empty());

  // Input string is space and we're trimming.
  r = SplitString(" ", ",", kTrimWhitespace, kSplitAll);
  ASSERT_EQ(1u, r.size());
  EXPECT_EQ("", r[0]);
  r = SplitString(" ", ",", kTrimWhitespace, kSplitNonempty);
  EXPECT_TRUE(r.empty());

  // Test all 4 combinations of flags on ", ,".
  r = SplitString(", ,", ",", kKeepWhitespace, kSplitAll);
  ASSERT_EQ(3u, r.size());
  EXPECT_EQ("", r[0]);
  EXPECT_EQ(" ", r[1]);
  EXPECT_EQ("", r[2]);
  r = SplitString(", ,", ",", kKeepWhitespace, kSplitNonempty);
  ASSERT_EQ(1u, r.size());
  ASSERT_EQ(" ", r[0]);
  r = SplitString(", ,", ",", kTrimWhitespace, kSplitAll);
  ASSERT_EQ(3u, r.size());
  EXPECT_EQ("", r[0]);
  EXPECT_EQ("", r[1]);
  EXPECT_EQ("", r[2]);
  r = SplitString(", ,", ",", kTrimWhitespace, kSplitNonempty);
  ASSERT_TRUE(r.empty());
}

// Tests for TrimString
TEST(StringUtils, TrimString) {
  // Basic tests
  EXPECT_EQ("a", TrimString("a"));
  EXPECT_EQ("a", TrimString(" a"));
  EXPECT_EQ("a", TrimString("a "));
  EXPECT_EQ("a", TrimString(" a "));

  // Tests with empty strings
  EXPECT_EQ("", TrimString(""));
  EXPECT_EQ("", TrimString(" \n\r\t"));
  EXPECT_EQ(" foo ", TrimString(" foo ", ""));

  // Tests it doesn't removes characters in the middle
  EXPECT_EQ("foo bar", TrimString(" foo bar "));

  // Test with non-whitespace trimChars
  EXPECT_EQ(" ", TrimString("foo bar", "abcdefghijklmnopqrstuvwxyz"));
}

TEST(StringUtils, RemoveComments) {
  EXPECT_EQ("", RemoveComments("", ""));
  EXPECT_EQ("", RemoveComments("foo", ""));

  EXPECT_EQ("foo", RemoveComments("foo #wwedw"));
  EXPECT_EQ("foo", RemoveComments("foo //ff", "//"));
  EXPECT_EQ("", RemoveComments("//ff", "//"));
}

TEST(StringUtils, StartsWith) {
  ASSERT_FALSE(StartsWith("foo", "bar"));
  ASSERT_FALSE(StartsWith("", "foo"));
  ASSERT_FALSE(StartsWith("foo", "foobar"));

  ASSERT_TRUE(StartsWith("foobar", "foo"));
  ASSERT_TRUE(StartsWith("foobar", ""));
  ASSERT_TRUE(StartsWith("foo", "foo"));
  ASSERT_TRUE(StartsWith("", ""));
}

TEST(StringUtils, EndsWith) {
  ASSERT_FALSE(EndsWith("foo", "bar"));
  ASSERT_FALSE(EndsWith("", "bar"));
  ASSERT_FALSE(EndsWith("foo", "foobar"));

  ASSERT_TRUE(EndsWith("foobar", "bar"));
  ASSERT_TRUE(EndsWith("foobar", ""));
  ASSERT_TRUE(EndsWith("bar", "bar"));
  ASSERT_TRUE(EndsWith("", ""));
}

}   // namespace
