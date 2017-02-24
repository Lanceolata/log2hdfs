// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_COMPRESS_H_
#define LOG2HDFS_KAFKA2HDFS_COMPRESS_H_

#include <string>
#include <memory>
#include "util/queue.h"

namespace log2hdfs {

class PathFormat;
class FpCache;

class Compress {
 public:
  enum Type {
    kUnCompress,
    kLzo,
    kOrc
  };

  static std::unique_ptr<Compress> Init(
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
      std::shared_ptr<Queue<std::string> > upload_queue);

  virtual ~Compress() {}

  virtual void Start() = 0;

  virtual void Stop() = 0;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_COMPRESS_H_
