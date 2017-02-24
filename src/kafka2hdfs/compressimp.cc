// Copyright (c) 2017 Lanceolata

#include "kafka2hdfs/compressimp.h"
#include <unistd.h>
#include <string.h>
#include <string>
#include <vector>
#include <memory>
#include "kafka2hdfs/path_format.h"
#include "util/fp_cache.h"
#include "util/logger.h"
#include "util/system_utils.h"

namespace log2hdfs {

namespace {

bool ArgsValid(const std::string &local_dir,
               off_t max_size,
               unsigned int move_interval,
               std::shared_ptr<PathFormat> path_format,
               std::shared_ptr<FpCache> fp_cache,
               unsigned int compress_interval,
               const std::string &upload_dir,
               std::shared_ptr<Queue<std::string> > upload_queue) {
  if (local_dir.empty() || upload_dir.empty()) {
    return false;
  }

  if (max_size == 0 || move_interval == 0 || compress_interval || 0) {
    return false;
  }

  if (!path_format || !path_format.get() || !fp_cache || !fp_cache.get()
          || !upload_queue || !upload_queue.get()) {
    return false;
  }
  return true;
}

}   // namespace

// ------------------------------------------------------------------
// Compress

std::unique_ptr<Compress> Compress::Init(
    Compress::Type type,
    const std::string &local_dir,
    off_t max_size,
    unsigned int move_interval,
    std::shared_ptr<PathFormat> path_format,
    std::shared_ptr<FpCache> fp_cache,
    const std::string &compress_dir,
    unsigned int compress_interval,
    const std::string &compress_cmd,
    const std::string &upload_dir,
    std::shared_ptr<Queue<std::string> > upload_queue) {

  std::unique_ptr<Compress> res;
  switch (type) {
    case kUnCompress:
      res = UnCompress::Init(local_dir, max_size, move_interval,
                             path_format, fp_cache, compress_dir,
                             compress_interval, compress_cmd,
                             upload_dir, upload_queue);
      break;
    case kLzo:
      res = LzoCompress::Init(local_dir, max_size, move_interval,
                              path_format, fp_cache, compress_dir,
                              compress_interval, compress_cmd,
                              upload_dir, upload_queue);
      break;
    case kOrc:
      res = OrcCompress::Init(local_dir, max_size, move_interval,
                              path_format, fp_cache, compress_dir,
                              compress_interval, compress_cmd,
                              upload_dir, upload_queue);
      break;
    default:
      res = nullptr;
      Log(LogLevel::kLogError, "Unknown compress type");
  }
  return res;
}

// ------------------------------------------------------------------
// CompressImp

bool CompressImp::WriteFinish(const char *filename, const char *path) const {
  Optional<time_t> ts = path_format_->ExtractTimeStampFromFilename(filename);
  if (!ts.valid()) {
    Log(LogLevel::kLogWarn, "Extract timestamp from filename[%s] failed",
        filename);
    return false;
  }

  time_t ts_now = time(NULL);
  if (ts_now <= ts.value()) {
    return false;
  }

  Optional<time_t> mtime = FileMtime(path);
  if (!mtime.valid()) {
    Log(LogLevel::kLogError, "FileMtime[%s] failed with errno[%d]",
        path, errno);
    return false;
  }

  if (ts_now - mtime.value() > move_interval_) {
    return true;
  }
  return false;
}

bool CompressImp::ReachSize(const char *path) const {
  Optional<off_t> filesize = FileSize(path);
  if (!filesize.valid()) {
    Log(LogLevel::kLogError, "FileSize filep[%s] failed", path);
    return false;
  }
  return filesize.value() > max_size_;
}

// ------------------------------------------------------------------
// OrcCompress

std::unique_ptr<Compress> OrcCompress::Init(
    const std::string &local_dir,
    off_t max_size,
    unsigned int move_interval,
    std::shared_ptr<PathFormat> path_format,
    std::shared_ptr<FpCache> fp_cache,
    const std::string &compress_dir,
    unsigned int compress_interval,
    const std::string &compress_cmd,
    const std::string &upload_dir,
    std::shared_ptr<Queue<std::string> > upload_queue) {
  bool res = ArgsValid(local_dir, max_size, move_interval, path_format,
                       fp_cache, compress_interval, upload_dir,
                       upload_queue);
  if (!res) {
    return nullptr;
  }
  if (compress_dir.empty()) {
    return nullptr;
  }
  return std::unique_ptr<Compress>(new OrcCompress(
              local_dir, max_size, move_interval, path_format, fp_cache,
              compress_dir, compress_interval, compress_cmd, upload_dir,
              upload_queue));
}

void OrcCompress::Start() {
  Log(LogLevel::kLogInfo, "orc compress start with local_dir[%s] "
      "compress_dir[%s] upload_dir[%s] cmd[%s]", local_dir_.c_str(),
      compress_dir_.c_str(), upload_dir_.c_str(), compress_cmd_.c_str());

  running_ = true;
  while (running_) {
    MoveToCompress();
    MoveToUpload();
    sleep(compress_interval_);
  }

  Log(LogLevel::kLogInfo, "orc compress stop with local_dir[%s] "
      "compress_dir[%s] upload_dir[%s] cmd[%s]", local_dir_.c_str(),
      compress_dir_.c_str(), upload_dir_.c_str(), compress_cmd_.c_str());
}

void OrcCompress::Stop() {
  running_ = false;
}

void OrcCompress::MoveToCompress() {
  Optional<std::vector<std::string> > filenames =
      ScanDirFile(local_dir_.c_str(), NULL, NULL);
  if (!filenames.valid()) {
    Log(LogLevel::kLogError, "scandirfile dir[%s] failed with errno[%d]",
        local_dir_.c_str(), errno);
    return;
  }

  if (filenames.value().empty()) {
    return;
  }

  char local_path[512], local_path2[512], compress_path[512];
  int m = snprintf(local_path, sizeof(local_path), "%s/",
                   local_dir_.c_str());
  int n = snprintf(compress_path, sizeof(compress_path), "%s/compress/",
                   compress_dir_.c_str());

  if (m < 0 || n < 0) {
    Log(LogLevel::kLogError, "snprintf failed local_dir[%s] compress_dir[%s]",
        local_dir_.c_str(), compress_dir_.c_str());
    return;
  }
  memcpy(local_path2, local_path, m);

  for (std::string filename : filenames.value()) {
    int temp = snprintf(local_path + m, sizeof(local_path) - m, "%s",
                        filename.c_str());
    if (temp < 0) {
      Log(LogLevel::kLogError, "snprintf failed with filename[%s]",
          filename.c_str());
      continue;
    }

    if (ReachSize(local_path) || WriteFinish(filename.c_str(), local_path)) {
      time_t ts = time(NULL);
      temp = snprintf(local_path2 + m, sizeof(local_path2) - m, "%s_%ld",
                      filename.c_str(), ts);
      if (temp < 0) {
        Log(LogLevel::kLogError, "snprintf local_path2 failed path[%s]",
            local_path);
        continue;
      }

      if (!Rename(local_path, local_path2)) {
        Log(LogLevel::kLogError, "Rename from[%s] to[%s] failed",
            local_path, local_path2);
        continue;
      }

      fp_cache_->Remove(filename);

      temp = snprintf(compress_path + n, sizeof(compress_path) - n, "%s_%ld",
                      filename.c_str(), ts);
      if (temp < 0) {
        Log(LogLevel::kLogError, "snprintf compress_path failed file[%s]",
            filename.c_str());
        continue;
      }

      if (!Rename(local_path2, compress_path)) {
        Log(LogLevel::kLogError, "Rename from[%s] to[%s] failed",
            local_path2, compress_path);
        continue;
      }
    }
  }
}

void OrcCompress::MoveToUpload() {
  char compressed_path[512];
  int m = snprintf(compressed_path, sizeof(compressed_path), "%s/compressed/",
                   compress_dir_.c_str());
  if (m < 0) {
    Log(LogLevel::kLogError, "snprintf compressed failed [%s]",
        compress_dir_.c_str());
    return;
  }

  Optional<std::vector<std::string> > filenames =
      ScanDirFile(compressed_path, NULL, NULL);
  if (!filenames.valid()) {
    Log(LogLevel::kLogError, "scandirfile dir[%s] failed with errno[%d]",
        compressed_path, errno);
    return;
  }

  if (filenames.value().empty()) {
    return;
  }

  char upload_path[512];
  int n = snprintf(upload_path, sizeof(upload_path), "%s/",
                   upload_dir_.c_str());
  for (std::string filename : filenames.value()) {
    int temp = snprintf(compressed_path + m, sizeof(compressed_path) - m,
                        "%s", filename.c_str());
    if (temp < 0) {
      Log(LogLevel::kLogError, "snprintf compressed_path failed [%s]",
          filename.c_str());
      return;
    }

    temp = snprintf(upload_path + n, sizeof(upload_path) - n, "%s",
                    filename.c_str());
    if (temp < 0) {
      Log(LogLevel::kLogError, "snprintf upload_path failed [%s]",
          filename.c_str());
      return;
    }
    if (!Rename(compressed_path, upload_path)) {
      Log(LogLevel::kLogError, "Rename from[%s] to[%s] failed",
          compressed_path, upload_path);
      return;
    }
    upload_queue_->Push(upload_path);
  }
}

// ------------------------------------------------------------------
// LzoCompress

std::unique_ptr<Compress> LzoCompress::Init(
    const std::string &local_dir,
    off_t max_size,
    unsigned int move_interval,
    std::shared_ptr<PathFormat> path_format,
    std::shared_ptr<FpCache> fp_cache,
    const std::string &compress_dir,
    unsigned int compress_interval,
    const std::string &compress_cmd,
    const std::string &upload_dir,
    std::shared_ptr<Queue<std::string> > upload_queue) {
  bool res = ArgsValid(local_dir, max_size, move_interval, path_format,
                       fp_cache, compress_interval, upload_dir,
                       upload_queue);
  if (!res) {
    return nullptr;
  }
  if (compress_cmd.empty() || compress_dir.empty()) {
    return nullptr;
  }
  return std::unique_ptr<Compress>(new LzoCompress(
              local_dir, max_size, move_interval, path_format, fp_cache,
              compress_dir, compress_interval, compress_cmd, upload_dir,
              upload_queue));
}

void LzoCompress::Start() {
  Log(LogLevel::kLogInfo, "lzo compress start with local_dir[%s] "
      "compress_dir[%s] upload_dir[%s] cmd[%s]", local_dir_.c_str(),
      compress_dir_.c_str(), upload_dir_.c_str(), compress_cmd_.c_str());

  running_ = true;
  while (running_) {
    MoveToCompress();
    MoveToUpload();
    sleep(compress_interval_);
  }

  Log(LogLevel::kLogInfo, "lzo compress stop with local_dir[%s] "
      "compress_dir[%s] upload_dir[%s] cmd[%s]", local_dir_.c_str(),
      compress_dir_.c_str(), upload_dir_.c_str(), compress_cmd_.c_str());
}

void LzoCompress::Stop() {
  running_ = false;
}

void LzoCompress::MoveToCompress() {
  Optional<std::vector<std::string> > filenames =
      ScanDirFile(local_dir_.c_str(), NULL, NULL);
  if (!filenames.valid()) {
    Log(LogLevel::kLogError, "scandirfile dir[%s] failed with errno[%d]",
        local_dir_.c_str(), errno);
    return;
  }

  if (filenames.value().empty()) {
    return;
  }

  char local_path[512], compress_path[512];
  int m = snprintf(local_path, sizeof(local_path), "%s/",
                   local_dir_.c_str());
  int n = snprintf(compress_path, sizeof(compress_path), "%s/compress/",
                   compress_dir_.c_str());

  if (m < 0 || n < 0) {
    Log(LogLevel::kLogError, "snprintf failed local_dir[%s] compress_dir[%s]",
        local_dir_.c_str(), compress_dir_.c_str());
    return;
  }

  for (std::string filename : filenames.value()) {
    int temp = snprintf(local_path + m, sizeof(local_path) - m, "%s",
                        filename.c_str());
    if (temp < 0) {
      Log(LogLevel::kLogError, "snprintf failed with filename[%s]",
          filename.c_str());
      continue;
    }

    if (ReachSize(local_path) || WriteFinish(filename.c_str(), local_path)) {
      temp = snprintf(compress_path + n, sizeof(compress_path) - n,
                      "%s_%ld", filename.c_str(), time(NULL));
      if (temp < 0) {
        Log(LogLevel::kLogError, "snprintf compress_path failed with file[%s]",
            filename.c_str());
        continue;
      }

      if (!Rename(local_path, compress_path)) {
        Log(LogLevel::kLogError, "Rename from[%s] to[%s] failed",
            local_path, compress_path);
        continue;
      }

      fp_cache_->Remove(filename);
      Lzop(compress_path);
    }
  }
}

bool LzoCompress::Lzop(const char *path) {
  char cmd[512];
  int n = snprintf(cmd, sizeof(cmd), "%s %s && mv %s.lzo %s/compressed/ &",
                   compress_cmd_.c_str(), path, path, compress_dir_.c_str());
  if (n < 0) {
    Log(LogLevel::kLogError, "snprintf Lzop cmd failed with path[%s]",
        path);
    return false;
  }

  std::string errstr;
  if (!ExecuteCommand(cmd, &errstr)) {
    Log(LogLevel::kLogError, "Exec cmd[%s] failed %s", cmd, errstr.c_str());
    return false;
  }
  return true;
}

void LzoCompress::MoveToUpload() {
  char compressed_path[512];
  int m = snprintf(compressed_path, sizeof(compressed_path), "%s/compressed/",
                   compress_dir_.c_str());
  if (m < 0) {
    Log(LogLevel::kLogError, "snprintf compressed failed [%s]",
        compress_dir_.c_str());
    return;
  }

  Optional<std::vector<std::string> > filenames =
      ScanDirFile(compressed_path, NULL, NULL);
  if (!filenames.valid()) {
    Log(LogLevel::kLogError, "scandirfile dir[%s] failed with errno[%d]",
        compressed_path, errno);
    return;
  }

  if (filenames.value().empty()) {
    return;
  }

  char upload_path[512];
  int n = snprintf(upload_path, sizeof(upload_path), "%s/",
                   upload_dir_.c_str());
  for (std::string filename : filenames.value()) {
    int temp = snprintf(compressed_path + m, sizeof(compressed_path) - m,
                        "%s", filename.c_str());
    if (temp < 0) {
      Log(LogLevel::kLogError, "snprintf compressed_path failed [%s]",
          filename.c_str());
      return;
    }

    temp = snprintf(upload_path + n, sizeof(upload_path) - n, "%s",
                    filename.c_str());
    if (temp < 0) {
      Log(LogLevel::kLogError, "snprintf upload_path failed [%s]",
          filename.c_str());
      return;
    }
    if (!Rename(compressed_path, upload_path)) {
      Log(LogLevel::kLogError, "Rename from[%s] to[%s] failed",
          compressed_path, upload_path);
      return;
    }
    upload_queue_->Push(upload_path);
  }
}

// ------------------------------------------------------------------
// UnCompress

std::unique_ptr<Compress> UnCompress::Init(
    const std::string &local_dir,
    off_t max_size,
    unsigned int move_interval,
    std::shared_ptr<PathFormat> path_format,
    std::shared_ptr<FpCache> fp_cache,
    const std::string &compress_dir,
    unsigned int compress_interval,
    const std::string &compress_cmd,
    const std::string &upload_dir,
    std::shared_ptr<Queue<std::string> > upload_queue) {
  bool res = ArgsValid(local_dir, max_size, move_interval, path_format,
                       fp_cache, compress_interval, upload_dir,
                       upload_queue);
  if (!res) {
    return nullptr;
  }
  return std::unique_ptr<Compress>(new UnCompress(
              local_dir, max_size, move_interval, path_format, fp_cache,
              compress_dir, compress_interval, compress_cmd, upload_dir,
              upload_queue));
}

void UnCompress::Start() {
  Log(LogLevel::kLogInfo, "un compress start with local_dir[%s] "
      "compress_dir[%s] upload_dir[%s] cmd[%s]", local_dir_.c_str(),
      compress_dir_.c_str(), upload_dir_.c_str(), compress_cmd_.c_str());

  running_ = true;
  while (running_) {
    MoveToUpload();
    sleep(compress_interval_);
  }

  Log(LogLevel::kLogInfo, "un compress stop with local_dir[%s] "
      "compress_dir[%s] upload_dir[%s] cmd[%s]", local_dir_.c_str(),
      compress_dir_.c_str(), upload_dir_.c_str(), compress_cmd_.c_str());
}

void UnCompress::Stop() {
  running_ = false;
}

void UnCompress::MoveToUpload() {
  Optional<std::vector<std::string> > filenames =
      ScanDirFile(local_dir_.c_str(), NULL, NULL);
  if (!filenames.valid()) {
    Log(LogLevel::kLogError, "scandirfile dir[%s] failed with errno[%d]",
        local_dir_.c_str(), errno);
    return;
  }

  if (filenames.value().empty()) {
    return;
  }

  char local_path[512], upload_path[512];;
  int m = snprintf(local_path, sizeof(local_path), "%s/",
                   local_dir_.c_str());
  int n = snprintf(upload_path, sizeof(upload_path), "%s/",
                   upload_dir_.c_str());

  if (m < 0 || n < 0) {
    Log(LogLevel::kLogError, "snprintf failed local_dir[%s] upload_dir[%s]",
        local_dir_.c_str(), upload_dir_.c_str());
    return;
  }

  for (std::string filename : filenames.value()) {
    int temp = snprintf(local_path + m, sizeof(local_path) - m, "%s",
                        filename.c_str());
    if (temp < 0) {
      Log(LogLevel::kLogError, "snprintf failed with filename[%s]",
          filename.c_str());
      continue;
    }

    if (ReachSize(local_path) || WriteFinish(filename.c_str(), local_path)) {
      temp = snprintf(upload_path + n, sizeof(upload_path) - n,
                      "%s_%ld.seq", filename.c_str(), time(NULL));
      if (temp < 0) {
        Log(LogLevel::kLogError, "snprintf upload_path failed with file[%s]",
            filename.c_str());
        continue;
      }

      if (!Rename(local_path, upload_path)) {
        Log(LogLevel::kLogError, "Rename from[%s] to[%s] failed",
            local_path, upload_path);
        continue;
      }

      fp_cache_->Remove(filename);
      upload_queue_->Push(upload_path);
    }
  }
}

}   // namespace log2hdfs
