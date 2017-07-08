// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_PATH_FORMAT_H_
#define LOG2HDFS_KAFKA2HDFS_PATH_FORMAT_H_

#include <string>
#include <memory>
#include "util/optional.h"

namespace log2hdfs {

class KafkaMessage;
class LogFormat;
class TopicConf;

/**
 * Path format interface
 */
class PathFormat {
 public:
  /**
   * Path format type enum
   */
  enum Type {
    kNormal,
  };

  /**
   * Convert type string to PathFormat Type
   */
  static Optional<PathFormat::Type> ParseType(const std::string &type);

  /**
   * Static function to create a PathFormat shared_ptr
   */
  static std::shared_ptr<PathFormat> Init(std::shared_ptr<TopicConf> conf);

  virtual ~PathFormat() {}

  /**
   * Build local file name from kafka message
   * 
   * @param msg                 kafka message
   * @param name                name to set
   * 
   * @returns True if build local file name success, false otherwise.
   */
  virtual bool BuildLocalFileName(const KafkaMessage& msg,
                                  std::string* name) const = 0;

  /**
   * Whether local file is write finished.
   * 
   * @param filepath            local file path
   * 
   * @returns True if file wirte finished, false otherwise.
   */
  virtual bool WriteFinished(const std::string& filepath) const = 0;

  /**
   * Build hdfs path from local file name.
   * 
   * @param name                local file name
   * @param path                hdfs path to set
   * 
   * @returns True if build hdfs path success, false otherwise.
   */
  virtual bool BuildHdfsPath(const std::string& name,
                             std::string* path,
                             bool delay = false) const = 0;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_PATH_FORMAT_H_
