// Copyright (c) 2017 Lanceolata

#include "kafka2hdfs/uploadimp.h"
#include <sstream>
#include "kafka2hdfs/path_format.h"
#include "util/logger.h"
#include "util/system_utils.h"
#include "util/string_utils.h"

namespace log2hdfs {

namespace {

bool ArgsValid(std::shared_ptr<Queue<std::string> > upload_queue,
               std::shared_ptr<PathFormat> path_format,
               std::shared_ptr<HdfsHandle> fs_handle,
               const std::string &upload_dir) {
  if (upload_dir.empty()) {
    return false;
  }

  if (!upload_queue || !upload_queue.get()
          || !path_format || !path_format.get()
          || !fs_handle || !fs_handle.get()) {
    return false;
  }
  return true;
}

#define HDFS_PUT "hadoop fs -put"

bool HdfsPut(const char *path, const char *hdfs_path) {
  char cmd[1024];
  int n = snprintf(cmd, sizeof(cmd), "%s %s %s && rm -f %s &",
                   HDFS_PUT, path, hdfs_path, path);
  if (n <= 0) {
    Log(LogLevel::kLogError, "snprintf HdfsPut cmd failed path[%s] "
        "hdfs_path[%s]", path, hdfs_path);
    return false;
  }
  Log(LogLevel::kLogInfo, "HdfsPut exec cmd[%s]", cmd);
  std::string errstr;
  bool res = ExecuteCommand(cmd, &errstr);
  if (!res) {
    Log(LogLevel::kLogWarn, "ExecuteCommand[%s] failed with errstr[%s]",
        cmd, errstr.c_str());
  }
  return res;
}

#define HDFS_APPEND "hadoop fs -appendToFile"

bool HdfsAppend(const char *path, const char *hdfs_path) {
  char cmd[1024];
  int n = snprintf(cmd, sizeof(cmd), "%s %s %s && rm -f %s &",
                   HDFS_APPEND, path, hdfs_path, path);
  if (n <= 0) {
    Log(LogLevel::kLogError, "snprintf HdfsAppend cmd failed path[%s] "
        "hdfs_path[%s]", path, hdfs_path);
    return false;
  }
  Log(LogLevel::kLogInfo, "HdfsAppend exec cmd[%s]", cmd);
  std::string errstr;
  bool res = ExecuteCommand(cmd, &errstr);
  if (!res) {
    Log(LogLevel::kLogWarn, "ExecuteCommand[%s] failed with errstr[%s]",
        cmd, errstr.c_str());
  }
  return res;
}

bool HdfsPutAndIndex(const char *path, const char *hdfs_path,
                     const char *index_cmd) {
  char cmd[1024];
  int n = snprintf(cmd, sizeof(cmd), "%s %s %s && rm -f %s && %s %s &",
                   HDFS_PUT, path, hdfs_path, path, index_cmd, hdfs_path);
  if (n <= 0) {
    Log(LogLevel::kLogError, "snprintf HdfsPutAndIndex cmd failed path[%s] "
        "hdfs_path[%s] index_cmd[%s]", path, hdfs_path, index_cmd);
    return false;
  }
  Log(LogLevel::kLogInfo, "HdfsPut exec cmd[%s]", cmd);
  std::string errstr;
  bool res = ExecuteCommand(cmd, &errstr);
  if (!res) {
    Log(LogLevel::kLogWarn, "ExecuteCommand[%s] failed with errstr[%s]",
        cmd, errstr.c_str());
  }
  return res;
}

}   // namespace

// ------------------------------------------------------------------
// Upload

std::unique_ptr<Upload> Upload::Init(
    Upload::Type type,
    std::shared_ptr<Queue<std::string> > upload_queue,
    std::shared_ptr<PathFormat> path_format,
    std::shared_ptr<HdfsHandle> fs_handle,
    const std::string &upload_dir,
    const std::string &index_cmd) {

  std::unique_ptr<Upload> res = nullptr;
  switch (type) {
    case kText:
      res = TextUpload::Init(upload_queue, path_format, fs_handle, upload_dir);
      break;
    case kLzo:
      res = LzoUpload::Init(upload_queue, path_format, fs_handle,
                            upload_dir, index_cmd);
      break;
    case kOrc:
      res = OrcUpload::Init(upload_queue, path_format, fs_handle, upload_dir);
      break;
    default:
      Log(LogLevel::kLogError, "Unknown Upload type");
      break;
  }
  return res;
}

// ------------------------------------------------------------------
// UploadImp

bool UploadImp::HdfsCreateDirectory(const std::string &path) const {
  size_t found = path.find_last_of("/");
  if (found == std::string::npos) {
    return false;
  }
  std::string dir_path = path.substr(0, found);
  return fs_handle_->CreateDirectory(dir_path.c_str());
}

// ------------------------------------------------------------------
// TextUpload

std::unique_ptr<Upload> TextUpload::Init(
    std::shared_ptr<Queue<std::string> > upload_queue,
    std::shared_ptr<PathFormat> path_format,
    std::shared_ptr<HdfsHandle> fs_handle,
    const std::string &upload_dir) {
  bool res = ArgsValid(upload_queue, path_format, fs_handle, upload_dir);
  if (!res) {
    return nullptr;
  }
  return std::unique_ptr<Upload>(new TextUpload(upload_queue, path_format,
                                                fs_handle, upload_dir));
}

void TextUpload::Start() {
  Log(LogLevel::kLogInfo, "TextUpload start with upload_dir[%s]",
      upload_dir_.c_str());

  std::string local_path;
  running_ = true;
  while (running_) {
    upload_queue_->WaitPop(&local_path);
    if (local_path.empty()) {
      Log(LogLevel::kLogWarn, "WaitPop empty");
      continue;
    }

    Optional<std::string> hdfs_path =
        path_format_->BuildHdfsPathFromLocalpath(local_path);
    if (!hdfs_path.valid()) {
      Log(LogLevel::kLogWarn, "BuildHdfsPathFromLocalpath failed "
          "with path[%s]", local_path.c_str());
      continue;
    }

    const std::string &hdfs_path_ref = hdfs_path.value();

    if (HdfsExists(hdfs_path_ref.c_str())) {
      if (!HdfsAppend(local_path.c_str(), hdfs_path_ref.c_str())) {
        Log(LogLevel::kLogWarn, "HdfsAppend local_path[%s] to hdfs_path[%s] "
            "failed", local_path.c_str(), hdfs_path_ref.c_str());
      }
    } else {
      if (!HdfsCreateDirectory(hdfs_path_ref)) {
        Log(LogLevel::kLogWarn, "HdfsCreateDirectory failed with path[%s]",
            hdfs_path_ref.c_str());
        continue;
      }
      if (!HdfsPut(local_path.c_str(), hdfs_path_ref.c_str())) {
        Log(LogLevel::kLogWarn, "HdfsPut local_path[%s] to hdfs_path[%s] "
            "failed", local_path.c_str(), hdfs_path_ref.c_str());
      }
    }
  }

  Log(LogLevel::kLogInfo, "TextUpload stop with upload_dir[%s]",
      upload_dir_.c_str());
}

void TextUpload::Stop() {
  running_ = false;
}

// ------------------------------------------------------------------
// LzoUpload

std::unique_ptr<Upload> LzoUpload::Init(
    std::shared_ptr<Queue<std::string> > upload_queue,
    std::shared_ptr<PathFormat> path_format,
    std::shared_ptr<HdfsHandle> fs_handle,
    const std::string &upload_dir,
    const std::string &index_cmd) {
  bool res = ArgsValid(upload_queue, path_format, fs_handle, upload_dir);
  if (!res || index_cmd.empty()) {
    return nullptr;
  }
  return std::unique_ptr<Upload>(new LzoUpload(upload_queue, path_format,
                                               fs_handle, upload_dir,
                                               index_cmd));
}

void LzoUpload::Start() {
  Log(LogLevel::kLogInfo, "LzoUpload start with upload_dir[%s]",
      upload_dir_.c_str());

  std::string local_path, valid_hdfs_path;
  running_ = true;
  while (running_) {
    upload_queue_->WaitPop(&local_path);
    if (local_path.empty()) {
      Log(LogLevel::kLogWarn, "WaitPop empty");
      continue;
    }

    Optional<std::string> hdfs_path =
        path_format_->BuildHdfsPathFromLocalpath(local_path);
    if (!hdfs_path.valid()) {
      Log(LogLevel::kLogWarn, "BuildHdfsPathFromLocalpath failed "
          "with path[%s]", local_path.c_str());
      continue;
    }

    const std::string &hdfs_path_ref = hdfs_path.value();

    if (!HdfsExists(hdfs_path_ref.c_str())) {
        if (!HdfsCreateDirectory(hdfs_path_ref)) {
          Log(LogLevel::kLogWarn, "HdfsCreateDirectory failed with path[%s]",
              hdfs_path_ref.c_str());
          continue;
        }
        valid_hdfs_path = hdfs_path_ref;
    } else {
      Log(LogLevel::kLogWarn, "hdfs path[%s] exists", hdfs_path_ref.c_str());
      valid_hdfs_path = hdfs_path_ref + "." + TsToString(time(NULL));
      while (HdfsExists(valid_hdfs_path.c_str())) {
        Log(LogLevel::kLogWarn, "hdfs path[%s] exists",
            valid_hdfs_path.c_str());
        valid_hdfs_path += "." + TsToString(time(NULL));
      }
    }

    if (!HdfsPutAndIndex(local_path.c_str(), valid_hdfs_path.c_str(),
                         index_cmd_.c_str())) {
        Log(LogLevel::kLogWarn, "HdfsPutAndIndex local_path[%s] to "
            "hdfs_path[%s] failed", local_path.c_str(),
            valid_hdfs_path.c_str());
    }
  }

  Log(LogLevel::kLogInfo, "LzoUpload stop with upload_dir[%s]",
      upload_dir_.c_str());
}

void LzoUpload::Stop() {
  running_ = false;
}

// ------------------------------------------------------------------
// OrcUpload

std::unique_ptr<Upload> OrcUpload::Init(
    std::shared_ptr<Queue<std::string> > upload_queue,
    std::shared_ptr<PathFormat> path_format,
    std::shared_ptr<HdfsHandle> fs_handle,
    const std::string &upload_dir) {
  bool res = ArgsValid(upload_queue, path_format, fs_handle, upload_dir);
  if (!res) {
    return nullptr;
  }
  return std::unique_ptr<Upload>(new OrcUpload(upload_queue, path_format,
                                               fs_handle, upload_dir));
}

void OrcUpload::Start() {
  Log(LogLevel::kLogInfo, "OrcUpload start with upload_dir[%s]",
      upload_dir_.c_str());

  std::string local_path, valid_hdfs_path, hdfs_dir_path;
  running_ = true;
  while (running_) {
    upload_queue_->WaitPop(&local_path);
    if (local_path.empty()) {
      Log(LogLevel::kLogWarn, "WaitPop empty");
      continue;
    }

    Optional<std::string> hdfs_path =
        path_format_->BuildHdfsPathFromLocalpath(local_path);
    if (!hdfs_path.valid()) {
      Log(LogLevel::kLogWarn, "BuildHdfsPathFromLocalpath failed "
          "with path[%s]", local_path.c_str());
      continue;
    }

    const std::string &hdfs_path_ref = hdfs_path.value();

    if (!HdfsExists(hdfs_path_ref.c_str())) {
        if (!HdfsCreateDirectory(hdfs_path_ref)) {
          Log(LogLevel::kLogWarn, "HdfsCreateDirectory failed with path[%s]",
              hdfs_path_ref.c_str());
          continue;
        }
        valid_hdfs_path = hdfs_path_ref;
    } else {
      Log(LogLevel::kLogWarn, "hdfs path[%s] exists", hdfs_path_ref.c_str());
      valid_hdfs_path = hdfs_path_ref + "." + TsToString(time(NULL));
      while (HdfsExists(valid_hdfs_path.c_str())) {
        Log(LogLevel::kLogWarn, "hdfs path[%s] exists",
            valid_hdfs_path.c_str());
        valid_hdfs_path += "." + TsToString(time(NULL));
      }
    }

    if (!HdfsPut(local_path.c_str(), valid_hdfs_path.c_str())) {
        Log(LogLevel::kLogWarn, "HdfsPut local_path[%s] to hdfs_path[%s] "
            "failed", local_path.c_str(), valid_hdfs_path.c_str());
    }
  }

  Log(LogLevel::kLogInfo, "OrcUpload stop with upload_dir[%s]",
      upload_dir_.c_str());
}

void OrcUpload::Stop() {
  running_ = false;
}

}   // namespace log2hdfs
