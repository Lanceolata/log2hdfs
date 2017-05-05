// Copyright (c) 2017 Lanceolata

#include "gtest/gtest.h"
#include <fstream>
#include "util/system_utils.h"
#include "easylogging++.h"

using namespace log2hdfs;

namespace {

TEST(SystemUtils, IsFile) {
  ASSERT_FALSE(IsFile(""));

  std::string file_path = "/tmp/isfile_" + std::to_string(time(NULL));
  ASSERT_FALSE(IsFile(file_path));
  std::ofstream out;
  out.open(file_path, std::ios::out | std::ios::app);
  out.close();
  ASSERT_TRUE(IsFile(file_path));

  std::string dir_path = "/tmp/isfile_" + std::to_string(time(NULL) + 2);
  MakeDir(dir_path);
  ASSERT_FALSE(IsFile(dir_path));
}

TEST(SystemUtils, RmFile) {
  std::string file_path = "/tmp/rmfile_" + std::to_string(time(NULL));
  std::ofstream out;
  out.open(file_path, std::ios::out | std::ios::app);
  out.close();
  ASSERT_TRUE(IsFile(file_path));
  ASSERT_TRUE(RmFile(file_path));
  ASSERT_FALSE(IsFile(file_path));
}

TEST(SystemUtils, FileSize) {
  Optional<off_t> fs = FileSize("");
  ASSERT_FALSE(fs.valid());

  std::string file_path = "/tmp/filesize_" + std::to_string(time(NULL));

  fs = FileSize(file_path);
  ASSERT_FALSE(fs.valid());

  std::ofstream out;
  out.open(file_path, std::ios::out | std::ios::app);
  out.close();

  fs = FileSize(file_path);
  ASSERT_TRUE(fs.valid());
  EXPECT_EQ(0, fs.value());

  out.open(file_path, std::ios::out | std::ios::app);
  out << "12345";
  out.close();

  fs = FileSize(file_path);
  ASSERT_TRUE(fs.valid());
  EXPECT_EQ(5, fs.value());
}

TEST(SystemUtils, FileMtime) {
  std::string file_path = "/tmp/filemtime_" + std::to_string(time(NULL));

  Optional<time_t> fm = FileMtime(file_path);
  ASSERT_FALSE(fm.valid());

  std::ofstream out;
  out.open(file_path, std::ios::out | std::ios::app);
  out.close();

  fm = FileMtime(file_path);
  ASSERT_TRUE(fm.valid());
  LOG(INFO) << "FileMtime:" << fm.value();
}

TEST(SystemUtils, BaseName) {
  Optional<std::string> bn = BaseName("");
  ASSERT_FALSE(bn.valid());

  std::string file_path = "/tmp/base_name";
  bn = BaseName(file_path);
  ASSERT_TRUE(bn.valid());
  EXPECT_EQ("base_name", bn.value());

  file_path = "base_name";
  bn = BaseName(file_path);
  ASSERT_TRUE(bn.valid());
  EXPECT_EQ("base_name", bn.value());
}

TEST(SystemUtils, IsDir) {
  ASSERT_FALSE(IsDir(""));

  std::string dir_path = "/tmp/isdir_" + std::to_string(time(NULL));
  ASSERT_FALSE(IsDir(dir_path));
  MakeDir(dir_path);
  ASSERT_TRUE(IsDir(dir_path));

  std::string file_path = "/tmp/isdir_" + std::to_string(time(NULL) + 2);
  std::ofstream out;
  out.open(file_path, std::ios::out | std::ios::app);
  out.close();
  ASSERT_FALSE(IsDir(file_path));
}

TEST(SystemUtils, MakeDir) {
  std::string dir_path = "/tmp/makedir_" + std::to_string(time(NULL));
  ASSERT_FALSE(IsDir(dir_path));
  ASSERT_TRUE(MakeDir(dir_path));
  ASSERT_TRUE(IsDir(dir_path));
}

TEST(SystemUtils, ScanDir) {
  std::string path = "/tmp";
  Optional<std::vector<std::string>> res = ScanDir(path, NULL, NULL);
  ASSERT_TRUE(res.valid());

  for (auto it : res.value()) {
    LOG(INFO) << it;
  }
}

TEST(SystemUtils, Rename) {
  std::string file_path1 = "/tmp/rename_" + std::to_string(time(NULL));
  std::string file_path2 = "/tmp/rename_" + std::to_string(time(NULL) + 2);
  std::ofstream out;
  out.open(file_path1, std::ios::out | std::ios::app);
  out.close();
  ASSERT_TRUE(IsFile(file_path1));
  ASSERT_FALSE(IsFile(file_path2));
  ASSERT_TRUE(Rename(file_path1, file_path2));
  ASSERT_FALSE(IsFile(file_path1));
  ASSERT_TRUE(IsFile(file_path2));
}

TEST(SystemUtils, StrToTs) {
  Optional<time_t> stt = StrToTs("", "");
  ASSERT_FALSE(stt.valid());

  stt = StrToTs("2017/4/21 14:34:4", "%Y/%m/%d %H:%M:%S");
  ASSERT_TRUE(stt.valid());
  EXPECT_EQ(1492756444, stt.value());
}


TEST(SystemUtils, ExecuteCommand) {
  std::string cmd = "touch /tmp/cmd_" + std::to_string(time(NULL));
  std::string error;
  ASSERT_TRUE(ExecuteCommand(cmd, &error));
}

}   // namespace
