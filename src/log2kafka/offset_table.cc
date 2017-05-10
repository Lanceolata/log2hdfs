// Copyright (c) 2017 Lanceolata

#include "log2kafka/offset_table.h"
#include <unistd.h>
#include <fstream>
#include "util/configparser.h"
#include "util/string_utils.h"
#include "easylogging++.h"

namespace log2hdfs {

#define DEFAULT_TABLE_PATH "offset_table"
#define DEFAULT_TABLE_INTERVAL "30"

std::shared_ptr<OffsetTable> OffsetTable::Init(
    std::shared_ptr<Section> section) {
  if (!section) {
    LOG(ERROR) << "OffsetTable Init invalid parameters";
    return nullptr;
  }

  std::string path = section->Get("table.path", DEFAULT_TABLE_PATH);
  if (path.empty()) {
    LOG(ERROR) << "OffsetTable Init empty path";
    return  nullptr;
  }

  std::string interval_str = section->Get("table.interval",
                                          DEFAULT_TABLE_INTERVAL);
  int interval = atoi(interval_str.c_str());
  if (interval <= 0) {
    LOG(ERROR) << "OffsetTable Init invalid interval[" << interval << "]";
    return nullptr;
  }

  LOG(INFO) << "OffsetTable Init parameters path[" << path
            << "] interval[" << interval << "]";
  return std::make_shared<OffsetTable>(path, interval);
}

bool OffsetTable::Update(const std::string& dir, const std::string& file,
                         off_t offset) {
  if (dir.empty() || file.empty() || offset < 0)
    return false;

  Fileoffset fo(file, offset);
  std::lock_guard<std::mutex> guard(mutex_);
  table_[dir] = std::move(fo);
  return true;
}

bool OffsetTable::Get(const std::string& dir, std::string* file,
                      off_t* offset) const {
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

bool OffsetTable::Remove(const std::string& dir) {
  if (dir.empty())
    return false;

  std::lock_guard<std::mutex> guard(mutex_);
  return table_.erase(dir);
}

bool OffsetTable::Save() const {
  FILE *fp = fopen(path_.c_str(), "w");
  if (fp == NULL) {
    LOG(ERROR) << "OffsetTable Save fopen path[" << path_
               << "] failed with errno[" << errno << "]";
    return false;
  }

  char buf[512];
  std::lock_guard<std::mutex> guard(mutex_);
  for (auto it = table_.begin(); it != table_.end(); ++it) {
    snprintf(buf, sizeof(buf), "%s/%s:%ld\n", it->first.c_str(),
             it->second.filename_.c_str(), it->second.offset_);
    int n = fputs(buf, fp);
    if (n == EOF) {
      LOG(ERROR) << "OffsetTable Save fputs path[" << path_
                 << "] value[" << buf << "] failed";
    }
  }
  fclose(fp);
  return true;
}

void OffsetTable::Remedy() {
  std::ifstream ifs(path_);
  if (!ifs.is_open()) {
    LOG(ERROR) << "OffsetTable Remedy ifstream path[" << path_
               << "] open failed";
    return;
  }

  std::string line;
  while (getline(ifs, line)) {
    std::vector<std::string> vec = SplitString(line, ":",
        kTrimWhitespace, kSplitNonempty);
    if (vec.size() != 2) {
      LOG(WARNING) << "OffsetTable Remedy invalid line[" << line << "]";
      continue;
    }

    if (!IsFile(vec[0])) {
      LOG(WARNING) << "OffsetTable Remedy line[" << line << "] not exists";
      continue;
    }

    std::string dir = DirName(vec[0]);
    std::string file = BaseName(vec[0]);
    off_t offset = atol(vec[1].c_str());
    if (dir.empty() || file.empty() || offset < 0) {
      LOG(WARNING) << "OffsetTable Remedy invalid line[" << line << "]";
      continue;
    }

    if (Update(dir, file, offset)) {
      LOG(INFO) << "OffsetTable Remedy construct record dir[" << dir
                << "] file[" << file << "] offset[" << offset << "]";
    } else {
      LOG(INFO) << "OffsetTable Remedy construct record dir[" << dir
                << "] file[" << file << "] offset[" << offset << "] failed";
    }
  }
  ifs.close();
}

void OffsetTable::StartInternal() {
  LOG(INFO) << "OffsetTable thread created";
  while (running_.load()) {
    sleep(interval_);
    Save();
  }
  LOG(INFO) << "OffsetTable thread existing";
}

}   // namespace log2hdfs
