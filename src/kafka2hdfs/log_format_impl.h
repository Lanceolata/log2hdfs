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

// ------------------------------------------------------------------
// EfLogFormat

class EfLogFormat : public LogFormat {
 public:
  static std::unique_ptr<EfLogFormat> Init();

  EfLogFormat() {}

  ~EfLogFormat() {}

  bool ExtractKeyAndTs(const char* payload, size_t len,
                       std::string* key, time_t* ts) const;

  bool ParseKey(const std::string& key,
                std::map<char, std::string>* m) const;
};

// ------------------------------------------------------------------
// EfDeviceLogFormat

class EfDeviceLogFormat : public LogFormat {
 public:
  static std::unique_ptr<EfDeviceLogFormat> Init();

  EfDeviceLogFormat() {}

  ~EfDeviceLogFormat() {}

  bool ExtractKeyAndTs(const char* payload, size_t len,
                       std::string* key, time_t* ts) const;

  bool ParseKey(const std::string& key,
                std::map<char, std::string>* m) const;
};

// ------------------------------------------------------------------
// ReportLogFormat

class ReportLogFormat : public LogFormat {
 public:
  static std::unique_ptr<ReportLogFormat> Init();

  ReportLogFormat() {}

  ~ReportLogFormat() {}

  bool ExtractKeyAndTs(const char* payload, size_t len,
                       std::string* key, time_t* ts) const;

  bool ParseKey(const std::string& key,
                std::map<char, std::string>* m) const;
};

// ------------------------------------------------------------------
// EfIcLogFormat

class EfIcLogFormat : public LogFormat {
 public:
  static std::unique_ptr<EfIcLogFormat> Init();

  EfIcLogFormat() {}

  ~EfIcLogFormat() {}

  bool ExtractKeyAndTs(const char* payload, size_t len,
                       std::string* key, time_t* ts) const;

  bool ParseKey(const std::string& key,
                std::map<char, std::string>* m) const;
};

// ------------------------------------------------------------------
// EfIcAwsLogFormat

class EfIcAwsLogFormat : public LogFormat {
 public:
  static std::unique_ptr<EfIcAwsLogFormat> Init();

  EfIcAwsLogFormat() {}

  ~EfIcAwsLogFormat() {}

  bool ExtractKeyAndTs(const char* payload, size_t len,
                       std::string* key, time_t* ts) const;

  bool ParseKey(const std::string& key,
                std::map<char, std::string>* m) const;
};

// ------------------------------------------------------------------
// EfImpLogFormat

class EfImpLogFormat : public LogFormat {
 public:
  static std::unique_ptr<EfImpLogFormat> Init();

  EfImpLogFormat() {}

  ~EfImpLogFormat() {}

  bool ExtractKeyAndTs(const char* payload, size_t len,
                       std::string* key, time_t* ts) const;

  bool ParseKey(const std::string& key,
                std::map<char, std::string>* m) const;
};

// ------------------------------------------------------------------
// EfStatsLogFormat

class EfStatsLogFormat : public LogFormat {
 public:
  static std::unique_ptr<EfStatsLogFormat> Init();

  EfStatsLogFormat() {}

  ~EfStatsLogFormat() {}

  bool ExtractKeyAndTs(const char* payload, size_t len,
                       std::string* key, time_t* ts) const;

  bool ParseKey(const std::string& key,
                std::map<char, std::string>* m) const;
};

// ------------------------------------------------------------------
// EfPubLogFormat

class EfPubLogFormat : public LogFormat {
 public:
  static std::unique_ptr<EfPubLogFormat> Init();

  EfPubLogFormat() {}

  ~EfPubLogFormat() {}

  bool ExtractKeyAndTs(const char* payload, size_t len,
                       std::string* key, time_t* ts) const;

  bool ParseKey(const std::string& key,
                std::map<char, std::string>* m) const;
};

}   // namespace log2hdfs

#endif  // LOG2HDFS_KAFKA2HDFS_LOG_FORMAT_IMPL_H_
