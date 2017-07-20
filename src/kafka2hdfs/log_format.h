// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_KAFKA2HDFS_LOG_FORMAT_H_
#define LOG2HDFS_KAFKA2HDFS_LOG_FORMAT_H_

#include <string>
#include <map>
#include <memory>
#include "util/optional.h"

namespace log2hdfs {

/**
 * Log format interface
 */
class LogFormat {
 public:
  /**
   * Log format type enum
   */
  enum Type {
    kV6,
    kV6Device,
    kEf,
    kEfDevice,
    kEfIc,
    kEfIcAws,
    kEfImp,
    kEfStats,
    kPub,
    kReport,
    kPreBid
  };

  /**
   * Convert type string to LogFormat type
   */
  static Optional<LogFormat::Type> ParseType(const std::string &type);

  /**
   * Static function to create LogFormat unique_ptr.
   */
  static std::unique_ptr<LogFormat> Init(LogFormat::Type type);

  virtual ~LogFormat() {}

  /**
   * Extract key and timestamp from kafka message payload.
   * 
   * @param payload             kafka message payload
   * @param len                 payload len
   * @param key                 key to set
   * @param ts                  timestamp to set
   * 
   * @return extract success return true, otherwise false,
   */
  virtual bool ExtractKeyAndTs(const char* payload, size_t len,
                               std::string* key, time_t* ts) const = 0;

  /**
   * Parse key to map
   * 
   * @param key                 key to parse
   * @param m                   map to set
   * 
   * @returns True if parse success, false otherwise.
   */
  virtual bool ParseKey(const std::string& key,
                        std::map<char, std::string>* m) const = 0;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_LOG_FORMAT_H_
