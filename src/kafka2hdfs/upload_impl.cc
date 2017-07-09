// Copyright (c) 2017 Lanceolata

#include "kafka2hdfs/upload_impl.h"
#include <unistd.h>
#include "kafka2hdfs/hdfs_handle.h"
#include "kafka2hdfs/path_format.h"
#include "kafka2hdfs/topic_conf.h"
#include "util/fp_cache.h"
#include "util/system_utils.h"
#include "util/string_utils.h"
#include "easylogging++.h"

namespace log2hdfs {

namespace {

int scandir_filter(const struct dirent *dep) {
  if (strncmp(dep->d_name, ".", 1) == 0)
    return 0;
  return 1;
}

int scandir_compar(const struct dirent **a, const struct dirent **b) {
  return strcmp((*a)->d_name, (*b)->d_name);
}

time_t GetZeroTs(time_t ts) {
  struct tm tm;
  localtime_r(&ts, &tm);
  tm.tm_sec = 0;
  tm.tm_min = 0;
  tm.tm_hour = 0;
  return mktime(&tm);
}

void ScandirAndPushQueue(const std::string& dir, Queue<std::string>* que) {
  std::vector<std::string> names;
  if (!ScanDir(dir, scandir_filter, scandir_compar, &names)) {
    LOG(WARNING) << "ScandirAndPushQueue ScanDir[" << dir << "] failed "
                 << "with errno[" << errno << "]";
  } else {
    for (auto& name : names) {
      std::string path = dir + "/" + name;
      if (!IsFile(path))
        continue;

      LOG(INFO) << "ScandirAndPushQueue Push file[" << path << "] success";
      que->Push(path);
    }
  }
}

bool DirParametersCheck(const std::string name, std::shared_ptr<TopicConf> conf) {
  std::string consume_dir = conf->consume_dir();
  if (consume_dir.empty() || !MakeDir(consume_dir)) {
    LOG(ERROR) << name << " Init invalid consume_dir["
               << consume_dir << "]";
    return false;
  }

  std::string compress_dir = conf->compress_dir();
  if (compress_dir.empty() || !MakeDir(compress_dir)) {
    LOG(ERROR) << name << " Init invalid compress_dir["
               << compress_dir << "]";
    return false;
  }

  std::string upload_dir = conf->upload_dir();
  if (upload_dir.empty() || !MakeDir(upload_dir)) {
    LOG(ERROR) << name << " Init invalid upload_dir["
               << upload_dir << "]";
    return false;
  }
  return true;
}

}   // namespace

// ------------------------------------------------------------------
// Upload

Optional<Upload::Type> Upload::ParseType(const std::string& type) {
  if (type == "text") {
    return Optional<Upload::Type>(kText);
  } else if (type == "lzo") {
    return Optional<Upload::Type>(kLzo);
  } else if (type == "orc") {
    return Optional<Upload::Type>(kOrc);
  } else if (type == "compress") {
    return Optional<Upload::Type>(kCompress);
  } else if (type == "appendcvt") {
    return Optional<Upload::Type>(kAppendCvt);
  } else if (type == "textnoupload") {
    return Optional<Upload::Type>(kTextNoUpload);
  } else {
    return Optional<Upload::Type>::Invalid();
  }
}

std::unique_ptr<Upload> Upload::Init(
    std::shared_ptr<TopicConf> conf,
    std::shared_ptr<PathFormat> format,
    std::shared_ptr<FpCache> fp_cache,
    std::shared_ptr<HdfsHandle> handle) {
  Upload::Type type = conf->upload_type();
  switch (type) {
    case kText:
      return TextUploadImpl::Init(std::move(conf), std::move(format),
                 std::move(fp_cache), std::move(handle));
    case kLzo:
      return LzoUploadImpl::Init(std::move(conf), std::move(format),
                 std::move(fp_cache), std::move(handle));
    case kOrc:
      return OrcUploadImpl::Init(std::move(conf), std::move(format),
                 std::move(fp_cache), std::move(handle));
    case kCompress:
      return CompressUploadImpl::Init(std::move(conf), std::move(format),
                 std::move(fp_cache), std::move(handle));
    case kAppendCvt:
      return AppendCvtUploadImpl::Init(std::move(conf), std::move(format),
                 std::move(fp_cache), std::move(handle));
    case kTextNoUpload:
      return TextNoUploadImpl::Init(std::move(conf), std::move(format),
                 std::move(fp_cache), std::move(handle));
    default:
      return nullptr;
  }
}

// ------------------------------------------------------------------
// UploadImpl

void UploadImpl::StartInternal() {
  LOG(INFO) << "UploadImpl topic[" << topic_ << "] thread created";

  Remedy();

  std::vector<std::string> names;
  while (!stop_.load()) {
    sleep(conf_->upload_interval());
    names.clear();

    if (!ScanDir(consume_dir_, scandir_filter, scandir_compar, &names)) {
      LOG(WARNING) << "UploadImpl StartInternal ScanDir[" << consume_dir_
                   << "] failed with errno[" << errno << "]";
      continue;
    }

    for (auto& name : names) {
      std::string path = consume_dir_ + "/" + name;
      if (!IsFile(path))
        continue;
    
      if (format_->WriteFinished(path)) {
        // Get fp cache key
        auto end = name.rfind(".");
        if (end == std::string::npos) {
          LOG(WARNING) << "UploadImpl StartInternal invalid path["
                       << path << "]";
          continue;
        }

        // Remove from fp cache
        FpCache::RemoveResult res = fp_cache_->Remove(
            name.substr(0, end), path);
        if (res == FpCache::kRemoveFailed) {
          LOG(ERROR) << "UploadImpl StartInternal fp_cache Remove["
                     << path << "] failed";
          continue;
        } else if (res == FpCache::kInvalidKey) {
          LOG(WARNING) << "UploadImpl StartInternal fp_cache Remove["
                       << path << "] invalid key";
        } else {
          LOG(INFO) << "UploadImpl StartInternal fp_cache Remove["
                    << path << "] success";
        }

        // Rename to compress dir
        std::string new_path = compress_dir_ + "/" + name;
        if (!Rename(path, new_path)) {
          LOG(WARNING) << "UploadImpl StartInternal Rename from[" << path
                       << "] to [" << new_path << "] failed with errno["
                       << errno << "]";
          continue;
        }

        // push new path to compress queue
        compress_queue_.Push(new_path);
      }
    
    }

    Compress();
    Upload();
  }

  LOG(INFO) << "UploadImpl topic[" << topic_ << "] thread existing";
}

void UploadImpl::Remedy() {
  ScandirAndPushQueue(compress_dir_, &compress_queue_);
  ScandirAndPushQueue(upload_dir_, &upload_queue_);
}

void UploadImpl::UploadFile(const std::string& path, bool append,
                            bool index, bool delay) {
  if (path.empty()) {
    LOG(WARNING) << "UploadImpl UploadPath topic[" << topic_
                 << "] empty path";
    return;
  }

  size_t found = path.find(":");
  int times = 0;
  std::string file_path;
  if (found != std::string::npos) {
    times = atoi(path.substr(found + 1).c_str());
    file_path = path.substr(0, found);
  } else {
    file_path = path;
  }

  if (!IsFile(file_path)) {
    LOG(WARNING) << "UploadImpl UploadPath invalid path[" << file_path << "]";
    return;
  }

  std::string name = BaseName(file_path);
  std::string hdfs_path;
  if (!format_->BuildHdfsPath(name, &hdfs_path, delay)) {
    LOG(WARNING) << "UploadImpl UploadPath BuildHdfsPath[" << file_path
                 << "] failed";
    return;
  }

  if (times > 0) {
    auto end = hdfs_path.rfind('.');
    if (end == std::string::npos) {
      hdfs_path += "." + std::to_string(times);
    } else {
      std::string temp_path = hdfs_path.substr(0, end) +
          std::to_string(times) + hdfs_path.substr(end);
      hdfs_path = std::move(temp_path);
    }
  }

  bool res;
  if (handle_->Exists(hdfs_path)) {
    if (append) {
      res = handle_->Append(file_path, hdfs_path);
    } else {
      LOG(WARNING) << "UploadImpl UploadFile file_path[" << file_path
                   << "] hdfs path[" << hdfs_path << "] already exists";
      return;
    }
  } else {
    std::string dir = DirName(hdfs_path);
    if (!handle_->CreateDirectory(dir)) {
      LOG(WARNING) << "UploadImpl UploadPath CreateDirectory[" << dir
                   << "] failed";
      return;
    } else {
      res = handle_->Put(file_path, hdfs_path);
    }
  }

  if (!res) {
    LOG(WARNING) << "UploadImpl UploadPath[" << file_path << "] to["
                 << hdfs_path << "] failed retry";
    ++times;
    upload_queue_.Push(file_path + ":" + std::to_string(times));
  } else {
    if (!RmFile(file_path)) {
      LOG(WARNING) << "UploadImpl UploadPath RmFile[" << file_path
                   << "] failed";
    }
    
    if (index) {
      sleep(3);
      handle_->LZOIndex(hdfs_path);
    }

    LOG(INFO) << "UploadImpl UploadPath[" << file_path << "] to["
              << hdfs_path << "] success";
  }
}

// ------------------------------------------------------------------
// TextUploadImpl

std::unique_ptr<TextUploadImpl> TextUploadImpl::Init(
    std::shared_ptr<TopicConf> conf,
    std::shared_ptr<PathFormat> format,
    std::shared_ptr<FpCache> fp_cache,
    std::shared_ptr<HdfsHandle> handle) {
  if (!conf || !format || !fp_cache || !handle) {
    LOG(ERROR) << "TextUploadImpl Init invalid parameters";
    return nullptr;
  }

  if (!DirParametersCheck("TextUploadImpl", conf)) {
    LOG(ERROR) << "TextUploadImpl Init DirParametersCheck failed";
    return nullptr;
  }

  return std::unique_ptr<TextUploadImpl>(new TextUploadImpl(
             std::move(conf), std::move(format), std::move(fp_cache),
             std::move(handle)));
}

void TextUploadImpl::Compress() {
  std::string path;
  while (compress_queue_.TryPop(&path)) {
    std::string name = BaseName(path);
    if (name.empty()) {
      LOG(WARNING) << "TextUploadImpl Compress empty path";
      continue;
    }

    // Rename to upload dir
    std::string new_path = upload_dir_ + "/" + name;
    if (!Rename(path, new_path)) {
      LOG(WARNING) << "TextUploadImpl Compress Rename from["
                   << path << "] to [" << new_path << "] failed "
                   << "with errno[" << errno << "]";
      continue;
    }

    upload_queue_.Push(new_path);
  }
}

void TextUploadImpl::Upload() {
  std::string path;
  while (upload_queue_.TryPop(&path)) {
    pool_.Enqueue([this](const std::string p) {
                    this->UploadFile(p, true, false);
                  }, path);
  }
}

// ------------------------------------------------------------------
// LzoUploadImpl

std::unique_ptr<LzoUploadImpl> LzoUploadImpl::Init(
    std::shared_ptr<TopicConf> conf,
    std::shared_ptr<PathFormat> format,
    std::shared_ptr<FpCache> fp_cache,
    std::shared_ptr<HdfsHandle> handle) {
  if (!conf || !format || !fp_cache || !handle) {
    LOG(ERROR) << "LzoUploadImpl Init invalid parameters";
    return nullptr;
  }

  if (!DirParametersCheck("LzoUploadImpl", conf)) {
    LOG(ERROR) << "LzoUploadImpl Init DirParametersCheck failed";
    return nullptr;
  }

  std::string lzo_cmd = conf->compress_lzo();
  if (lzo_cmd.empty()) {
    LOG(ERROR) << "LzoUploadImpl Init invalid lzo_cmd";
    return nullptr;
  }

  return std::unique_ptr<LzoUploadImpl>(new LzoUploadImpl(
             std::move(conf), std::move(format), std::move(fp_cache),
             std::move(handle)));
}

void LzoUploadImpl::Compress() {
  std::string path;
  while (compress_queue_.TryPop(&path)) {
    std::string name = BaseName(path);
    if (name.empty()) {
      LOG(WARNING) << "LzoUploadImpl Compress empty path";
      continue;
    }

    pool_.Enqueue([this](const std::string p) {
                    this->CompressFile(p);
                  }, path);
  }
}

void LzoUploadImpl::CompressFile(const std::string& path) {
  if (path.empty() || !IsFile(path)) {
    LOG(WARNING) << "LzoUploadImpl CompressFile invalid path["
                 << path << "]";
    return;
  }

  std::string old_path;
  if (!EndsWith(path, ".lzo")) {
    std::string cmd = conf_->compress_lzo();
    cmd.append(" ");
    cmd.append(path);

    std::string errstr;
    bool res = ExecuteCommand(cmd, &errstr);
    if (!res) {
      LOG(ERROR) << "LzoUploadImpl CompressFile path["
                 << path << "] failed with errstr["
                 << errstr << "]";
      return;
    }
    old_path = path + ".lzo";
  } else {
    return;
  }

  std::string new_path = upload_dir_ + "/" + BaseName(old_path);
  if (!Rename(old_path, new_path)) {
    LOG(ERROR) << "LzoUploadImpl CompressFile Rename from[" << old_path
               << "] to[" << new_path << "] failed with errno[" << errno
               << "]";
    return;
  }

  upload_queue_.Push(new_path);
}

void LzoUploadImpl::Upload() {
  std::string path;
  while (upload_queue_.TryPop(&path)) {
    pool_.Enqueue([this](const std::string p) {
                    this->UploadFile(p, false, true);
                  }, path);
  }
}

// ------------------------------------------------------------------
// OrcUploadImpl

std::unique_ptr<OrcUploadImpl> OrcUploadImpl::Init(
    std::shared_ptr<TopicConf> conf,
    std::shared_ptr<PathFormat> format,
    std::shared_ptr<FpCache> fp_cache,
    std::shared_ptr<HdfsHandle> handle) {
  if (!conf || !format || !fp_cache || !handle) {
    LOG(ERROR) << "OrcUploadImpl Init invalid parameters";
    return nullptr;
  }

  if (!DirParametersCheck("OrcUploadImpl", conf)) {
    LOG(ERROR) << "OrcUploadImpl Init DirParametersCheck failed";
    return nullptr;
  }

  std::string orc_cmd = conf->compress_orc();
  if (orc_cmd.empty()) {
    LOG(ERROR) << "OrcUploadImpl Init invalid orc_cmd";
    return nullptr;
  }

  return std::unique_ptr<OrcUploadImpl>(new OrcUploadImpl(
             std::move(conf), std::move(format), std::move(fp_cache),
             std::move(handle)));
}

void OrcUploadImpl::Compress() {
  std::string path;
  while (compress_queue_.TryPop(&path)) {
    std::string name = BaseName(path);
    if (name.empty()) {
      LOG(WARNING) << "OrcUploadImpl Compress empty path";
      continue;
    }

    pool_.Enqueue([this](const std::string p) {
                    this->CompressFile(p);
                  }, path);
  }
}

void OrcUploadImpl::CompressFile(const std::string& path) {
  if (path.empty() || !IsFile(path)) {
    LOG(WARNING) << "OrcUploadImpl CompressFile invalid path["
                 << path << "]";
    return;
  }

  std::string old_path;
  if (!EndsWith(path, ".orc")) {
    std::string cmd = conf_->compress_orc();
    cmd.append(" ");
    cmd.append(path);

    std::string errstr;
    bool res = ExecuteCommand(cmd, &errstr);
    if (!res) {
      LOG(ERROR) << "OrcUploadImpl CompressFile path["
                 << path << "] failed with errstr["
                 << errstr << "]";
      return;
    }
    old_path = path + ".orc";
  } else {
    return;
  }

  std::string new_path = upload_dir_ + "/" + BaseName(old_path);
  if (!Rename(old_path, new_path)) {
    LOG(ERROR) << "OrcUploadImpl CompressFile Rename from[" << old_path
               << "] to[" << new_path << "] failed with errno[" << errno
               << "]";
    return;
  }

  upload_queue_.Push(new_path);
}

void OrcUploadImpl::Upload() {
  std::string path;
  while (upload_queue_.TryPop(&path)) {
    pool_.Enqueue([this](const std::string p) {
                    this->UploadFile(p, false, false);
                  }, path);
  }
}

// ------------------------------------------------------------------
// CompressUploadImpl

std::unique_ptr<CompressUploadImpl> CompressUploadImpl::Init(
    std::shared_ptr<TopicConf> conf,
    std::shared_ptr<PathFormat> format,
    std::shared_ptr<FpCache> fp_cache,
    std::shared_ptr<HdfsHandle> handle) {
  if (!conf || !format || !fp_cache || !handle) {
    LOG(ERROR) << "CompressUploadImpl Init invalid parameters";
    return nullptr;
  }

  if (!DirParametersCheck("CompressUploadImpl", conf)) {
    LOG(ERROR) << "CompressUploadImpl Init DirParametersCheck failed";
    return nullptr;
  }

  std::string mv_path = conf->compress_mv();
  if (mv_path.empty()) {
    LOG(ERROR) << "CompressUploadImpl Init invalid mv_path";
    return nullptr;
  }

  return std::unique_ptr<CompressUploadImpl>(new CompressUploadImpl(
             std::move(conf), std::move(format), std::move(fp_cache),
             std::move(handle)));
}

void CompressUploadImpl::Compress() {
  std::string mv_path = conf_->compress_mv();

  std::string path;
  while (compress_queue_.TryPop(&path)) {
    std::string name = BaseName(path);
    if (name.empty()) {
      LOG(WARNING) << "CompressUploadImpl Compress empty path";
      continue;
    }

    if (EndsWith(name, ".orc"))
      continue;

    std::string new_path = mv_path + "/" + name;
    if (!Rename(path, new_path)) {
      LOG(WARNING) << "CompressUploadImpl Compress Rename from[" << path
                   << "] to[" << new_path << "] failed with errno["
                   << errno << "]";
      continue;
    }
  }

  std::vector<std::string> names;
  if (!ScanDir(compress_dir_, scandir_filter, scandir_compar, &names)) {
    LOG(ERROR) << "CompressUploadImpl StartInternal ScanDir compres_dir["
               << compress_dir_ << "] failed with errno[" << errno << "]";
    return;
  }

  for (auto& name : names) {
    if (!EndsWith(name, ".orc"))
      continue;

    std::string old_path = compress_dir_ + "/" + name;
    std::string new_path = upload_dir_ + "/" + name;
    if (!Rename(old_path, new_path)) {
      LOG(WARNING) << "CompressUploadImpl Compress Rename from[" << old_path
                   << "] to[" << new_path << "] failed with errno["
                   << errno << "]";
      continue;
    }
    upload_queue_.Push(new_path);
  }
}

void CompressUploadImpl::Upload() {
  std::string path;
  while (upload_queue_.TryPop(&path)) {
    pool_.Enqueue([this](const std::string p) {
                    this->UploadFile(p, false, false);
                  }, path);
  }
}

// ------------------------------------------------------------------
// AppendCvtUploadImpl

std::unique_ptr<AppendCvtUploadImpl> AppendCvtUploadImpl::Init(
    std::shared_ptr<TopicConf> conf,
    std::shared_ptr<PathFormat> format,
    std::shared_ptr<FpCache> fp_cache,
    std::shared_ptr<HdfsHandle> handle) {
  if (!conf || !format || !fp_cache || !handle) {
    LOG(ERROR) << "AppendCvtUploadImpl Init invalid parameters";
    return nullptr;
  }

  if (!DirParametersCheck("AppendCvtUploadImpl", conf)) {
    LOG(ERROR) << "AppendCvtUploadImpl Init DirParametersCheck failed";
    return nullptr;
  }

  std::string append = conf->compress_appendcvt();
  if (append.empty()) {
    LOG(ERROR) << "AppendCvtUploadImpl Init invalid append";
    return nullptr;
  }

  std::string delay = conf->hdfs_path_delay();
  if (delay.empty()) {
    LOG(ERROR) << "AppendCvtUploadImpl Init invalid delay path";
    return nullptr;
  }

  return std::unique_ptr<AppendCvtUploadImpl>(new AppendCvtUploadImpl(
             std::move(conf), std::move(format), std::move(fp_cache),
             std::move(handle)));
}

bool AppendCvtUploadImpl::IsDelay(const std::string& name) {
  if (name.empty() || !StartsWith(name, "cvt..")) {
    LOG(WARNING) << "AppendCvtUploadImpl IsDelay invalid name[" << name << "]";
    return false;
  }

  std::string time_str = name.substr(5, 14);
  time_t time_stamp = StrToTs(time_str, "%Y%m%d%H%M%S");
  if (time_stamp <= 0) {
    LOG(WARNING) << "AppendCvtUploadImpl IsDelay name[" << name << "] "
                 << "StrToTs[" << time_str << "] failed";
    return false;
  }

  time_t max_ts = GetZeroTs(time_stamp) + 97200;
  if (time(NULL) > max_ts) {
    LOG(INFO) << "AppendCvtUploadImpl IsDelay Name[" << name << "] is delay";
    return true;
  }
  return false;
}

void AppendCvtUploadImpl::Compress() {
  time_t ts = time(NULL);
  time_t zero_ts = GetZeroTs(ts);
  time_t diff_ts = ts - zero_ts;
  if (diff_ts < 10860 && diff_ts > 10680) {
    LOG(INFO) << "AppendCvtUploadImpl Compress sleep 200s start";
    sleep(200);
    LOG(INFO) << "AppendCvtUploadImpl Compress sleep 200s end";
  }

  std::string path;
  while (compress_queue_.TryPop(&path)) {
    std::string name = BaseName(path);
    if (name.empty()) {
      LOG(WARNING) << "AppendCvtUploadImpl Compress empty path";
      continue;
    }

    if (IsDelay(name)) {
      pool_.Enqueue([this](const std::string p) {
                      this->CompressFile(p);
                    }, path);
    } else {
      std::string new_path = upload_dir_ + "/" + name;
      if (!Rename(path, new_path)) {
        LOG(WARNING) << "AppendCvtUploadImpl Compress Rename from["
                     << path << "] to [" << new_path << "] failed "
                     << "with errno[" << errno << "]";
        continue;
      }

      upload_queue_.Push(new_path);
    }
  }
}

void AppendCvtUploadImpl::CompressFile(const std::string& path) {
  if (path.empty() || !IsFile(path)) {
    LOG(WARNING) << "AppendCvtUploadImpl CompressFile invalid path["
                 << path << "]";
    return;
  }
  std::string old_path;
  if (!EndsWith(path, ".append")) {
    std::string cmd = conf_->compress_appendcvt();
    cmd.append(" ");
    cmd.append(path);

    std::string errstr;
    bool res = ExecuteCommand(cmd, &errstr);
    if (!res) {
      LOG(ERROR) << "AppendCvtUploadImpl CompressFile path["
                 << path << "] failed with errstr["
                 << errstr << "]";
    }
    old_path = path + ".append";
  } else {
    return;
  }

  std::string new_path = upload_dir_ + "/" + BaseName(old_path);
  if (!Rename(old_path, new_path)) {
    LOG(ERROR) << "AppendCvtUploadImpl CompressFile Rename from[" << old_path
               << "] to[" << new_path << "] failed with errno[" << errno
               << "]";
  }

  std::string new_path2 = upload_dir_ + "/" + BaseName(path);
  if (!Rename(path, new_path2)) {
    LOG(ERROR) << "AppendCvtUploadImpl CompressFile Rename from[" << path
               << "] to[" << new_path2 << "] failed with errno[" << errno
               << "]";
  }

  upload_queue_.Push(new_path);
  upload_queue_.Push(new_path2);
}

void AppendCvtUploadImpl::Upload() {
  std::string path;
  while (upload_queue_.TryPop(&path)) {
    if (EndsWith(path, ".append")) {
      pool_.Enqueue([this](const std::string p) {
                      this->UploadFile(p, true, false, true);
                    }, path);
    } else {
      pool_.Enqueue([this](const std::string p) {
                      this->UploadFile(p, true, false);
                    }, path);
    }
  }
}

// ------------------------------------------------------------------
// TextNoUpload

std::unique_ptr<TextNoUploadImpl> TextNoUploadImpl::Init(
    std::shared_ptr<TopicConf> conf,
    std::shared_ptr<PathFormat> format,
    std::shared_ptr<FpCache> fp_cache,
    std::shared_ptr<HdfsHandle> handle) {
  if (!conf || !format || !fp_cache || !handle) {
    LOG(ERROR) << "TextNoUploadImpl Init invalid parameters";
    return nullptr;
  }

  if (!DirParametersCheck("TextNoUploadImpl", conf)) {
    LOG(ERROR) << "TextNoUploadImpl Init DirParametersCheck failed";
    return nullptr;
  }

  return std::unique_ptr<TextNoUploadImpl>(new TextNoUploadImpl(
             std::move(conf), std::move(format), std::move(fp_cache),
             std::move(handle)));
}

void TextNoUploadImpl::Compress() {
  std::string path;
  while (compress_queue_.TryPop(&path)) {
    std::string name = BaseName(path);
    if (name.empty()) {
      LOG(WARNING) << "TextNoUploadImpl Compress empty path";
      continue;
    }

    std::string new_path = upload_dir_ + "/" + name;
    if (!Rename(path, new_path)) {
      LOG(WARNING) << "TextNoUploadImpl Compress Rename from[" << path
                   << "] to[" << new_path << "] failed with errno["
                   << errno << "]";
      continue;
    }
  }
}

void TextNoUploadImpl::Upload() {
  std::string path;
  while (upload_queue_.TryPop(&path)) {}
}

}   // namespace log2hdfs
