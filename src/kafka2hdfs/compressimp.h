// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_COMPRESSIMP_H_
#define LOG2HDFS_KAFKA2HDFS_COMPRESSIMP_H_

#include <string>
#include <memory>
#include "kafka2hdfs/compress.h"

namespace log2hdfs {

// ------------------------------------------------------------------
// CompressImp

class CompressImp : public Compress {
 public:
  CompressImp(off_t max_size, unsigned int move_interval,
              std::shared_ptr<PathFormat> path_format):
      max_size_(max_size), move_interval_(move_interval),
      path_format_(path_format) {}

  ~CompressImp() {}

 protected:
  bool WriteFinish(const char *filename, const char *path) const;

  bool ReachSize(const char *path) const;

  off_t max_size_;
  unsigned int move_interval_;
  std::shared_ptr<PathFormat> path_format_;
};

// ------------------------------------------------------------------
// OrcCompress

class OrcCompress : public CompressImp {
 public:
  static std::unique_ptr<Compress> Init(
      const std::string &local_dir,
      off_t max_size,
      unsigned int move_interval,
      std::shared_ptr<PathFormat> path_format,
      std::shared_ptr<FpCache> fp_cache,
      const std::string &compress_dir,
      unsigned int compress_interval,
      const std::string &compress_cmd,
      const std::string &upload_dir,
      std::shared_ptr<Queue<std::string> > upload_queue);

  OrcCompress(const OrcCompress &other) = delete;
  OrcCompress &operator=(const OrcCompress &other) = delete;

  ~OrcCompress() {}

  void Start();

  void Stop();

  void MoveToCompress();

  void MoveToUpload();

 private:
  OrcCompress(const std::string &local_dir,
              off_t max_size,
              unsigned int move_interval,
              std::shared_ptr<PathFormat> path_format,
              std::shared_ptr<FpCache> fp_cache,
              const std::string &compress_dir,
              unsigned int compress_interval,
              const std::string &compress_cmd,
              const std::string &upload_dir,
              std::shared_ptr<Queue<std::string> > upload_queue):
      CompressImp(max_size, move_interval, path_format),
      local_dir_(local_dir), fp_cache_(fp_cache),
      compress_dir_(compress_dir), compress_interval_(compress_interval),
      compress_cmd_(compress_cmd), upload_dir_(upload_dir),
      upload_queue_(upload_queue) {}

  std::string local_dir_;
  std::shared_ptr<FpCache> fp_cache_;
  std::string compress_dir_;
  unsigned int compress_interval_;
  std::string compress_cmd_;
  std::string upload_dir_;
  std::shared_ptr<Queue<std::string> > upload_queue_;
  bool running_ = false;
};

// ------------------------------------------------------------------
// LzoCompress

class LzoCompress : public CompressImp {
 public:
  static std::unique_ptr<Compress> Init(
      const std::string &local_dir,
      off_t max_size,
      unsigned int move_interval,
      std::shared_ptr<PathFormat> path_format,
      std::shared_ptr<FpCache> fp_cache,
      const std::string &compress_dir,
      unsigned int compress_interval,
      const std::string &compress_cmd,
      const std::string &upload_dir,
      std::shared_ptr<Queue<std::string> > upload_queue);

  LzoCompress(const LzoCompress &other) = delete;
  LzoCompress &operator=(const LzoCompress &other) = delete;

  ~LzoCompress() {}

  void Start();

  void Stop();

  void MoveToCompress();

  bool Lzop(const char *path);

  void MoveToUpload();

 private:
  LzoCompress(const std::string &local_dir,
              off_t max_size,
              unsigned int move_interval,
              std::shared_ptr<PathFormat> path_format,
              std::shared_ptr<FpCache> fp_cache,
              const std::string &compress_dir,
              unsigned int compress_interval,
              const std::string &compress_cmd,
              const std::string &upload_dir,
              std::shared_ptr<Queue<std::string> > upload_queue):
      CompressImp(max_size, move_interval, path_format),
      local_dir_(local_dir), fp_cache_(fp_cache),
      compress_dir_(compress_dir), compress_interval_(compress_interval),
      compress_cmd_(compress_cmd), upload_dir_(upload_dir),
      upload_queue_(upload_queue) {}

  std::string local_dir_;
  std::shared_ptr<FpCache> fp_cache_;
  std::string compress_dir_;
  unsigned int compress_interval_;
  std::string compress_cmd_;
  std::string upload_dir_;
  std::shared_ptr<Queue<std::string> > upload_queue_;
  bool running_ = false;
};


// ------------------------------------------------------------------
// UnCompress

class UnCompress : public CompressImp {
 public:
  static std::unique_ptr<Compress> Init(
      const std::string &local_dir,
      off_t max_size,
      unsigned int move_interval,
      std::shared_ptr<PathFormat> path_format,
      std::shared_ptr<FpCache> fp_cache,
      const std::string &compress_dir,
      unsigned int compress_interval,
      const std::string &compress_cmd,
      const std::string &upload_dir,
      std::shared_ptr<Queue<std::string> > upload_queue);

  UnCompress(const UnCompress &other) = delete;
  UnCompress &operator=(const UnCompress &other) = delete;

  ~UnCompress() {}

  void Start();

  void Stop();

  void MoveToUpload();

 private:
  UnCompress(const std::string &local_dir,
             off_t max_size,
             unsigned int move_interval,
             std::shared_ptr<PathFormat> path_format,
             std::shared_ptr<FpCache> fp_cache,
             const std::string &compress_dir,
             unsigned int compress_interval,
             const std::string &compress_cmd,
             const std::string &upload_dir,
             std::shared_ptr<Queue<std::string> > upload_queue):
      CompressImp(max_size, move_interval, path_format),
      local_dir_(local_dir), fp_cache_(fp_cache),
      compress_dir_(compress_dir), compress_interval_(compress_interval),
      compress_cmd_(compress_cmd), upload_dir_(upload_dir),
      upload_queue_(upload_queue) {}

  std::string local_dir_;
  std::shared_ptr<FpCache> fp_cache_;
  std::string compress_dir_;
  unsigned int compress_interval_;
  std::string compress_cmd_;
  std::string upload_dir_;
  std::shared_ptr<Queue<std::string> > upload_queue_;
  bool running_ = false;
};


}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_COMPRESSIMP_H_
