// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_LOG_FORMAT_IMPL_H_
#define LOG2HDFS_KAFKA2HDFS_LOG_FORMAT_IMPL_H_

#include "kafka2hdfs/log_format.h"

namespace log2hdfs {

// ------------------------------------------------------------------
// V6LogFormat

class V6LogFormat : public LogFormat {
 public:
  static std::shared_ptr<LogFormat> Init() {
    return std::shared_ptr<V6LogFormat>();
  }

  V6LogFormat() {}

  ~V6LogFormat() {}

  bool ExtractInfoFromPayload(
      const char *payload,
      time_t *ts,
      std::string *device,
      std::string *type) const;
};

// ------------------------------------------------------------------
// EfLogFormat

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_LOG_FORMAT_IMPL_H_
