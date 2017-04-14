// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_COMPRESS_H_
#define LOG2HDFS_KAFKA2HDFS_COMPRESS_H_

#include <string>
#include <memory>
#include "util/queue.h"
#include "util/optional.h"

namespace log2hdfs {

class Kafka2hdfsTopicConf;
class LogFormat;
class FpCache;
class CompressConf;

class Compress {
 public:
  enum Type {
    kUnCompress,
    kLzo,
    kOrc,
    kNone
  };

  static Optional<Compress::Type> GetTypeFromString(const std::string &type);

  static std::unique_ptr<Compress> Init(
      Compress::Type type, std::unique_ptr<Kafka2hdfsTopicConf> conf);

  virtual ~Compress() {}

  virtual void Start() = 0;

  virtual void Stop() = 0;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_COMPRESS_H_
