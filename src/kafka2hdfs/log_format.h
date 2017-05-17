// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_LOG_FORMAT_H_
#define LOG2HDFS_KAFKA2HDFS_LOG_FORMAT_H_

#include <string>
#include <map>
#include <memory>
#include "util/optional.h"

namespace log2hdfs {

class LogFormat {
 public:
  enum Type {
    kV6,
    kV6Device,
    kEf,
    kEfDevice,
    kReport
  };

  static Optional<LogFormat::Type> ParseType(const std::string &type);

  static std::unique_ptr<LogFormat> Init(LogFormat::Type type);

  virtual ~LogFormat() {}

  /**
   *  @return extract success return true, otherwise false,
   */
  virtual bool ExtractKeyAndTs(const char* payload, std::string* key,
                               time_t* ts) const = 0;

  virtual bool ParseKey(const std::string& key, 
                        std::map<std::string, std::string>* m) const = 0;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_LOG_FORMAT_H_
