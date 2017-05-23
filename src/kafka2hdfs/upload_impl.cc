// Copyright (c) 2017 Lanceolata

#include "kafka2hdfs/upload_impl.h"
#include <unistd.h>
#include "kafka2hdfs/hdfs_handle.h"
#include "kafka2hdfs/path_format.h"
#include "kafka2hdfs/topic_conf.h"
#include "util/fp_cache.h"
#include "util/system_utils.h"
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
  Upload::Type type = conf->file_format();
  switch (type) {
    case kText:
      return TextUploadImpl::Init(std::move(conf), std::move(format),
                 std::move(fp_cache), std::move(handle));
    case kLzo:
      return nullptr;
    case kOrc:
      return nullptr;
    case kCompress:
      return nullptr;
    case kTextNoUpload:
      return nullptr;
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
  while (running_.load()) {
    names.clear();
    if (!ScanDir(consume_dir, scandir_filter, scandir_compar, &names)) {
      LOG(WARNING) << "UploadImpl StartInternal ScanDir section[" << section
                   << "] consume_dir[" << consume_dir << "] failed with errno["
                   << errno << "]";
      sleep(conf_->upload_interval());
      continue;
    }

    for (auto& name : names) {
      std::string path = consume_dir + "/" + name;
      if (!IsFile(path)) {
        continue;
      }

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
        if (fp_cache_->Remove(name.substr(0, end)) == FpCache::kRemoveFailed) {
          LOG(WARNING) << "UploadImpl StartInternal section[" << section
                       << "] consume_dir[" << consume_dir << "] name["
                       << name << "] fp_cache_ Remove failed";
          continue;
        }

        // Rename to compress dir
        std::string new_path = compress_dir + "/" + name;
        if (!Rename(path, new_path)) {
          LOG(WARNING) << "UploadImpl StartInternal section[" << section
                       << "Rename from[" << path << "] to[" << new_path
                       << "] failed with errno[" << errno << "]";
          continue;
        }

        compress_queue_.Push(new_path);
      }
    }

    Compress();
    Upload();
    sleep(conf_->upload_interval());
  }

  LOG(INFO) << "UploadImpl section[" << section << "] thread existing";
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

  std::string consume_dir = conf->consume_dir();
  if (consume_dir.empty() || !MakeDir(consume_dir)) {
    LOG(ERROR) << "TextUploadImpl Init invalid consume_dir["
               << consume_dir << "]";
    return nullptr;
  }

  std::string compress_dir = conf->compress_dir();
  if (compress_dir.empty() || !MakeDir(compress_dir)) {
    LOG(ERROR) << "TextUploadImpl Init invalid compress_dir["
               << compress_dir << "]";
    return nullptr;
  }

  std::string upload_dir = conf->upload_dir();
  if (upload_dir.empty() || !MakeDir(upload_dir)) {
    LOG(ERROR) << "TextUploadImpl Init invalid upload_dir["
               << upload_dir << "]";
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
  std::string section = conf_->section();
  std::string path;
  while (upload_queue_.TryPop(&path)) {
    pool_.Enqueue([this](const std::string p) {
                    this->UploadPath(p);
                  }, path);
  }
}

void TextUploadImpl::UploadPath(const std::string& path) {
  if (!IsFile(path)) {
    LOG(WARNING) << "TextUploadImpl UploadPath invalid path["
                 << path << "]";
    return;
  }

  std::string name = BaseName(path);
  std::string hdfs_path;
  if (!format_->BuildHdfsPath(name, &hdfs_path)) {
    LOG(WARNING) << "TextUploadImpl UploadPath BuildHdfsPath[" << path
                 << "] failed";
    return;
  }

  bool res;
  if (handle_->Exists(hdfs_path)) {
    res = handle_->Append(path, hdfs_path);
  } else {
    std::string dir = DirName(hdfs_path);
    if (!handle_->CreateDirectory(dir)) {
      LOG(WARNING) << "TextUploadImpl UploadPath CreateDirectory[" << dir
                   << "] path[" << path << "] failed";
      res = true;
    } else {
      res = handle_->Put(path, hdfs_path);
    }
  }

  if (!res) {
    upload_queue_.Push(path);
  } else {
    if (!RmFile(path)) {
      LOG(WARNING) << "TextUploadImpl UploadPath RmFile[" << path
                   << "] failed";
    }
    LOG(INFO) << "TextUploadImpl UploadPath[" << path << "] to["
              << hdfs_path << "] success";
  }
}

}   // namespace log2hdfs
