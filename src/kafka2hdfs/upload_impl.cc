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

void ScandirAndPushQueue(std::string section, std::string dir, Queue<std::string>* que) {
  std::vector<std::string> names;
  if (!ScanDir(dir, scandir_filter, scandir_compar, &names)) {
    LOG(WARNING) << "UploadImpl Remedy ScanDir section[" << section
                 << "] dir[" << dir << "] failed "
                 << "with errno[" << errno << "]";
  } else {
    for (auto& name : names) {
      std::string path = dir + "/" + name;
      if (!IsFile(path))
        continue;

      LOG(INFO) << "UploadImpl Remedy file[" << path << "] success";
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
  } else if (type == "compress") {
    return Optional<Upload::Type>(kCompress);
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
    case kCompress:
      return CompressUploadImpl::Init(std::move(conf), std::move(format),
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
  std::string section = conf_->section();
  std::string consume_dir = conf_->consume_dir();
  std::string compress_dir = conf_->compress_dir();
  LOG(INFO) << "UploadImpl section[" << section << "] thread created";

  Remedy();

  std::vector<std::string> names;
  while (!stop_.load()) {
    sleep(conf_->upload_interval());
    names.clear();

    if (!ScanDir(consume_dir, scandir_filter, scandir_compar, &names)) {
      LOG(WARNING) << "UploadImpl StartInternal ScanDir section[" << section
                   << "] consume_dir[" << consume_dir << "] failed with errno["
                   << errno << "]";
      continue;
    }

    for (auto& name : names) {
      std::string path = consume_dir + "/" + name;
      if (!IsFile(path))
        continue;

      if (format_->WriteFinished(path)) {
        // Get fp cache key
        auto end = name.rfind(".");
        if (end == std::string::npos) {
          LOG(WARNING) << "UploadImpl StartInternal invalid section["
                       << section << "] consume_dir[" << consume_dir
                       << "] name[" << name << "]";
          continue;
        }

        // Remove from fp cache
        FpCache::RemoveResult res = fp_cache_->Remove(name.substr(0, end));
        if (res == FpCache::kRemoveFailed) {
          LOG(ERROR) << "UploadImpl StartInternal section[" << section
                     << "] consume_dir[" << consume_dir << "] name["
                     << name << "] fp_cache_ Remove failed";
          continue;
        } else if (res == FpCache::kInvalidKey) {
          LOG(WARNING) << "UploadImpl StartInternal section[" << section
                       << "] consume_dir[" << consume_dir << "] name["
                       << name << "] fp_cache_ Remove invalid key";
        } else {
          LOG(INFO) << "UploadImpl StartInternal section[" << section
                    << "] consume_dir[" << consume_dir << "] name["
                    << name << "] fp_cache_ Remove success";
        }

        // Rename to compress dir
        std::string new_path = compress_dir + "/" + name;
        if (!Rename(path, new_path)) {
          LOG(WARNING) << "UploadImpl StartInternal section[" << section
                       << "Rename from[" << path << "] to[" << new_path
                       << "] failed with errno[" << errno << "]";
          continue;
        }

        // push new path to compress queue
        compress_queue_.Push(new_path);
      }
    }

    Compress();
    Upload();
  }

  LOG(INFO) << "UploadImpl section[" << section << "] thread existing";
}

void UploadImpl::Remedy() {
  std::string section = conf_->section();
  std::string compress_dir = conf_->compress_dir();
  std::string upload_dir = conf_->upload_dir();
  std::vector<std::string> names;

  if (!IsDir(compress_dir)) {
    LOG(WARNING) << "UploadImpl Remedy failed invalid compress dir["
                 << compress_dir << "]";
  } else {
    ScandirAndPushQueue(section, compress_dir, &compress_queue_);
  }

  if (!IsDir(upload_dir)) {
    LOG(WARNING) << "UploadImpl Remedy failed invalid upload dir["
                 << compress_dir << "]";
  } else {
    ScandirAndPushQueue(section, upload_dir, &upload_queue_);
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
  std::string section = conf_->section();
  std::string upload_path = conf_->upload_dir();
  std::string path;
  while (compress_queue_.TryPop(&path)) {
    std::string name = BaseName(path);
    if (name.empty()) {
      LOG(WARNING) << "TextUploadImpl Compress invalid path["
                   << path << "]";
      continue;
    }

    // Rename to upload dir
    std::string new_path = upload_path + "/" + name;
    if (!Rename(path, new_path)) {
      LOG(WARNING) << "TextUploadImpl Compress section[" << section
                   << "Rename from[" << path << "] to[" << new_path
                   << "] failed with errno[" << errno << "]";
      continue;
    }

    upload_queue_.Push(new_path);
  }
}

void TextUploadImpl::Upload() {
  std::string path;
  while (upload_queue_.TryPop(&path)) {
    pool_.Enqueue([this](const std::string p) {
                    this->UploadFile(p);
                  }, path);
  }
}

void TextUploadImpl::UploadFile(const std::string& path) {
  if (path.empty()) {
    LOG(WARNING) << "TextUploadImpl UploadPath empty path";
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
    LOG(WARNING) << "TextUploadImpl UploadPath invalid path["
                 << file_path << "]";
    return;
  }

  std::string name = BaseName(file_path);
  std::string hdfs_path;
  if (!format_->BuildHdfsPath(name, &hdfs_path)) {
    LOG(WARNING) << "TextUploadImpl UploadPath BuildHdfsPath[" << file_path
                 << "] failed";
    return;
  }

  if (times > 0) {
    hdfs_path +=  "." + std::to_string(times);
  }

  bool res;
  if (handle_->Exists(hdfs_path)) {
    res = handle_->Append(file_path, hdfs_path);
  } else {
    std::string dir = DirName(hdfs_path);
    if (!handle_->CreateDirectory(dir)) {
      LOG(WARNING) << "TextUploadImpl UploadPath CreateDirectory[" << dir
                   << "] failed";
      return;
    } else {
      res = handle_->Put(file_path, hdfs_path);
    }
  }

  if (!res) {
    times++;
    upload_queue_.Push(file_path + ":" + std::to_string(times));
  } else {
    if (!RmFile(file_path)) {
      LOG(WARNING) << "TextUploadImpl UploadPath RmFile[" << file_path
                   << "] failed";
    }
    LOG(INFO) << "TextUploadImpl UploadPath[" << file_path << "] to["
              << hdfs_path << "] success";
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
  std::string section = conf_->section();
  std::string compress_path = conf_->compress_dir();
  std::string path;
  while (compress_queue_.TryPop(&path)) {
    std::string name = BaseName(path);
    if (name.empty()) {
      LOG(WARNING) << "TextUploadImpl Compress invalid path["
                   << path << "]";
      continue;
    }

    // Rename to upload dir
    std::string new_path = compress_path + "/" + name;
    if (!Rename(path, new_path)) {
      LOG(WARNING) << "TextUploadImpl Compress section[" << section
                   << "Rename from[" << path << "] to[" << new_path
                   << "] failed with errno[" << errno << "]";
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

  std::string cmd = conf_->compress_lzo();
  cmd.append(" ");
  cmd.append(path);
  std::string errstr;
  bool res = ExecuteCommand(cmd, &errstr);
  if (res) {
    std::string old_path = path + ".lzo";
    std::string new_path = conf_->upload_dir() + "/" + BaseName(old_path);
    if (!Rename(old_path, new_path)) {
      LOG(ERROR) << "LzoUploadImpl CompressFile Rename from[" << old_path
                 << "] to[" << new_path << "] failed with errno[" << errno
                 << "]";
      return;
    }
    upload_queue_.Push(new_path);
  } else {
    LOG(ERROR) << "LzoUploadImpl CompressFile path["
               << path << "] failed with errstr["
               << errstr << "]";
  }
}

void LzoUploadImpl::Upload() {
  std::string path;
  while (upload_queue_.TryPop(&path)) {
    pool_.Enqueue([this](const std::string p) {
                    this->UploadFile(p);
                  }, path);
  }
}

void LzoUploadImpl::UploadFile(const std::string& path) {
  if (path.empty()) {
    LOG(WARNING) << "LzoUploadImpl UploadPath empty path";
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
    LOG(WARNING) << "LzoUploadImpl UploadPath invalid path["
                 << file_path << "]";
    return;
  }

  std::string name = BaseName(file_path);
  std::string hdfs_path;
  if (!format_->BuildHdfsPath(name, &hdfs_path)) {
    LOG(WARNING) << "LzoUploadImpl UploadPath BuildHdfsPath[" << file_path
                 << "] failed";
    return;
  }

  if (times > 0) {
    hdfs_path += "." + std::to_string(times);
  }

  bool res;
  if (handle_->Exists(hdfs_path)) {
    LOG(WARNING) << "LzoUploadImpl UploadPath hdfs path["
                 << hdfs_path << "] already exists";
    return;
  } else {
    std::string dir = DirName(hdfs_path);
    if (!handle_->CreateDirectory(dir)) {
      LOG(WARNING) << "LzoUploadImpl UploadPath CreateDirectory[" << dir
                   << "] failed";
      return;
    } else {
      res = handle_->Put(file_path, hdfs_path);
    }
  }

  if (!res) {
    times++;
    upload_queue_.Push(file_path + ":" + std::to_string(times));
  } else {
    if (!RmFile(file_path)) {
      LOG(WARNING) << "LzoUploadImpl UploadPath RmFile[" << file_path
                   << "] failed";
    }
    handle_->LZOIndex(hdfs_path);
    LOG(INFO) << "LzoUploadImpl UploadPath[" << file_path << "] to["
              << hdfs_path << "] success";
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
  std::string section = conf_->section();
  std::string mv_path = conf_->compress_mv();
  std::string compress_path = conf_->compress_dir();
  std::string upload_path = conf_->upload_dir();
  std::string path;
  while (compress_queue_.TryPop(&path)) {
    std::string name = BaseName(path);
    if (name.empty()) {
      LOG(WARNING) << "CompressUploadImpl Compress invalid path["
                   << path << "]";
      continue;
    }

    // Rename to upload dir
    std::string new_path = mv_path + "/" + name;
    if (!Rename(path, new_path)) {
      LOG(WARNING) << "CompressUploadImpl Compress section[" << section
                   << "Rename from[" << path << "] to[" << new_path
                   << "] failed with errno[" << errno << "]";
      continue;
    }

  }

  std::vector<std::string> names;
  if (!ScanDir(compress_path, scandir_filter, scandir_compar, &names)) {
    LOG(WARNING) << "CompressUploadImpl StartInternal ScanDir section["
                 << section << "] compres_dir[" << compress_path
                 << "] failed with errno[" << errno << "]";
    return;
  }

  for (auto& name : names) {
    if (!EndsWith(name, ".orc"))
      continue;
    std::string old_path = compress_path + "/" + name;
    std::string new_path = upload_path + "/" + name;
    if (!Rename(old_path, new_path)) {
      LOG(WARNING) << "CompressUploadImpl Compress section[" << section
                   << "Rename from[" << old_path << "] to[" << new_path
                   << "] failed with errno[" << errno << "]";
      continue;
    }
    upload_queue_.Push(new_path);
  }
}

void CompressUploadImpl::Upload() {
  std::string path;
  while (upload_queue_.TryPop(&path)) {
    pool_.Enqueue([this](const std::string p) {
                    this->UploadFile(p);
                  }, path);
  }
}

void CompressUploadImpl::UploadFile(const std::string& path) {
  if (path.empty()) {
    LOG(WARNING) << "CompressUploadImpl UploadPath empty path";
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
    LOG(WARNING) << "CompressUploadImpl UploadPath invalid path["
                 << file_path << "]";
    return;
  }

  std::string name = BaseName(file_path);
  std::string hdfs_path;
  if (!format_->BuildHdfsPath(name, &hdfs_path)) {
    LOG(WARNING) << "CompressUploadImpl UploadPath BuildHdfsPath[" << file_path
                 << "] failed";
    return;
  }

  if (times > 0) {
    hdfs_path += "." + std::to_string(times);
  }

  bool res;
  if (handle_->Exists(hdfs_path)) {
    LOG(WARNING) << "CompressUploadImpl UploadPath hdfs path["
                 << hdfs_path << "] already exists";
    return;
  } else {
    std::string dir = DirName(hdfs_path);
    if (!handle_->CreateDirectory(dir)) {
      LOG(WARNING) << "CompressUploadImpl UploadPath CreateDirectory[" << dir
                   << "] failed";
      return;
    } else {
      res = handle_->Put(file_path, hdfs_path);
    }
  }

  if (!res) {
    times++;
    upload_queue_.Push(file_path + ":" + std::to_string(times));
  } else {
    if (!RmFile(file_path)) {
      LOG(WARNING) << "CompressUploadImpl UploadPath RmFile[" << file_path
                   << "] failed";
    }
    LOG(INFO) << "CompressUploadImpl UploadPath[" << file_path << "] to["
              << hdfs_path << "] success";
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
  std::string section = conf_->section();
  std::string upload_path = conf_->upload_dir();
  std::string path;
  while (compress_queue_.TryPop(&path)) {
    std::string name = BaseName(path);
    if (name.empty()) {
      LOG(WARNING) << "TextNoUploadImpl Compress invalid path["
                   << path << "]";
      continue;
    }

    // Rename to upload dir
    std::string new_path = upload_path + "/" + name;
    if (!Rename(path, new_path)) {
      LOG(WARNING) << "TextNoUploadImpl Compress section[" << section
                   << "Rename from[" << path << "] to[" << new_path
                   << "] failed with errno[" << errno << "]";
      continue;
    }

    upload_queue_.Push(new_path);
  }
}

void TextNoUploadImpl::Upload() {
  std::string path;
  while (upload_queue_.TryPop(&path)) {}
}

}   // namespace log2hdfs
