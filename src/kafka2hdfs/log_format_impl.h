// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_LOG_FORMAT_IMPL_H_
#define LOG2HDFS_KAFKA2HDFS_LOG_FORMAT_IMPL_H_

#include "kafka2hdfs/log_format.h"

namespace log2hdfs {

// ------------------------------------------------------------------
// V6LogFormat

class V6LogFormat : public LogFormat {
 public:
  static std::unique_ptr<V6LogFormat> Init();

  V6LogFormat() {}

  ~V6LogFormat() {}

  /**
   * @return true if extract success; otherwise false.
   */
  bool ExtractKeyAndTs(const char* payload, size_t len, 
                       std::string* key, time_t* ts) const;

  bool ParseKey(const std::string& key,
                std::map<char, std::string>* m) const;
};

// ------------------------------------------------------------------
// V6DeviceLogFormat

class V6DeviceLogFormat : public LogFormat {
 public:
  static std::unique_ptr<V6DeviceLogFormat> Init();

  V6DeviceLogFormat() {}

  ~V6DeviceLogFormat() {}

  bool ExtractKeyAndTs(const char* payload, size_t len,
                       std::string* key, time_t* ts) const;

  bool ParseKey(const std::string& key,
                std::map<char, std::string>* m) const;
};

}   // namespace log2hdfs

#endif  // namespace log2hdfs
