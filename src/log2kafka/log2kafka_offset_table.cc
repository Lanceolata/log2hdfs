// Copyright (c) 2017 Lanceolata

#include "log2kafka/log2kafka_offset_table.h"
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fstream>
#include "util/string_utils.h"
#include "easylogging++.h"

namespace log2hdfs {

std::shared_ptr<Log2kafkaOffsetTable> Log2kafkaOffsetTable::Init(
    const std::string& path, int interval) {
  if (path.empty() || interval <= 0)
    return nullptr;
  return std::make_shared<Log2kafkaOffsetTable>(path, interval);
}

bool Log2kafkaOffsetTable::Update(const std::string& dir,
    const std::string& file, off_t offset) {
  if (dir.empty() || file.empty() || offset < 0)
    return false;

  Fileoffset fo(file, offset);
  std::lock_guard<std::mutex> guard(mutex_);
  table_[dir] = std::move(fo);
  return true;
}

bool Log2kafkaOffsetTable::Get(const std::string& dir,
    std::string* file, off_t* offset) const {
  if (dir.empty() || !file || !offset)
    return false;

  std::lock_guard<std::mutex> guard(mutex_);
  auto it = table_.find(dir);
  if (it == table_.end())
    return false;

  *file = it->second.filename_;
  *offset = it->second.offset_;
  return true;
}

bool Log2kafkaOffsetTable::Remove(const std::string& dir) {
  if (dir.empty())
    return false;

  std::lock_guard<std::mutex> guard(mutex_);
  return table_.erase(dir);
}

bool Log2kafkaOffsetTable::Save() const {
  FILE *fp = fopen(path_.c_str(), "w");
  if (fp == NULL) {
    LOG(ERROR) << "Log2kafkaOffsetTable Save fopen path:" << path_
               << " failed with errno:" << errno;
    return false;
  }

  char buf[512];
  std::lock_guard<std::mutex> guard(mutex_);
  for (auto it = table_.begin(); it != table_.end(); ++it) {
    snprintf(buf, sizeof(buf), "%s/%s:%ld\n", it->first.c_str(),
             it->second.filename_.c_str(), it->second.offset_);
    int n = fputs(buf, fp);
    if (n == EOF) {
      LOG(ERROR) << "Log2kafkaOffsetTable Save fputs:" << path_
                 << " failed";
    }
  }
  fclose(fp);
  return true;
}

void Log2kafkaOffsetTable::Remedy() {
  std::ifstream ifs(path_);
  if (!ifs.is_open()) {
    LOG(WARNING) << "Log2kafkaOffsetTable Remedy ifstream path:"
                 << path_ << " open failed";
    return;
  }

  std::string line;
  while (getline(ifs, line)) {
    std::vector<std::string> vec = SplitString(line, ":",
        kTrimWhitespace, kSplitNonempty);
    if (vec.size() != 2) {
      LOG(WARNING) << "Log2kafkaOffsetTable Remedy invalid line:"
                   << line;
      continue;
    }

    if (!IsFile(vec[0])) {
      LOG(WARNING) << "Log2kafkaOffsetTable Remedy path:"
                   << line << " no file";
      continue;
    }

    Optional<std::string> dirname = DirName(vec[0]);
    Optional<std::string> basename = BaseName(vec[0]);
    off_t offset = atol(vec[1].c_str());
    if (!dirname.valid() || !basename.valid() || offset < 0) {
      LOG(WARNING) << "Log2kafkaOffsetTable Remedy invalid line:"
                   << line;
      continue;
    }

    if (Update(dirname.value(), basename.value(), offset)) {
      LOG(INFO) << "Log2kafkaOffsetTable Remedy construct record "
                << "dir[" << dirname.value() << "] file["
                << basename.value() << "] offset[" << offset << "]";
    } else {
      LOG(INFO) << "Log2kafkaOffsetTable Remedy construct record "
                << "dir[" << dirname.value() << "] file["
                << basename.value() << "] offset[" << offset << "] failed";
    }
  }
  ifs.close();
}

void Log2kafkaOffsetTable::Start() {
  std::lock_guard<std::mutex> guard(thread_mutex_);
  if (!thread_.joinable()) {
    running_.store(true);
    std::thread t(&Log2kafkaOffsetTable::StartInternal, this);
    thread_ = std::move(t);
  }
}

void Log2kafkaOffsetTable::StartInternal() {
  LOG(INFO) << "Log2kafkaOffsetTable thread created";
  while (running_.load()) {
    sleep(interval_);
    Save();
  }
  LOG(INFO) << "Log2kafkaOffsetTable thread existing";
}

}   // namespace log2hdfs
