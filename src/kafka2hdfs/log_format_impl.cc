// Copyright (c) 2017 Lanceolata

#include "kafka2hdfs/log_format_impl.h"
#include <string.h>
#include "util/system_utils.h"

namespace log2hdfs {

namespace {

bool ExtractString(const char* begin, const char* end,
                   const char delimeter, int num,
                   const char** rbegin, const char** rend) {
  if (!begin || !end || !rbegin || !rend)
    return false;

  const char* next = NULL;
  const char* temp = begin;
  int n = 0;
  while (temp != NULL && temp < end) {
    next = strchr(temp, delimeter);
    if (n == num)
      break;

    ++n;
    if (next) {
      temp = next + 1;
    } else {
      temp = NULL;
    }
  }

  if (temp == NULL || temp >= end)
    return false;

  *rbegin = temp;
  if (next == NULL) {
    *rend = end;
  } else {
    *rend = next;
  }
  return true;
}

}   // namespace

// ------------------------------------------------------------------
// LogFormat

Optional<LogFormat::Type> LogFormat::ParseType(const std::string &type) {
  if (type == "v6") {
    return Optional<LogFormat::Type>(kV6);
  } else if (type == "v6device") {
    return Optional<LogFormat::Type>(kV6Device);
  } else if (type == "ef") {
    return Optional<LogFormat::Type>(kEf);
  } else if (type == "efdevice") {
    return Optional<LogFormat::Type>(kEfDevice);
  } else if (type == "report") {
    return Optional<LogFormat::Type>(kReport);
  } else if (type == "efic") {
    return Optional<LogFormat::Type>(kEfIc);
  } else if (type == "efimp") {
    return Optional<LogFormat::Type>(kEfImp);
  } else if (type == "efstats") {
    return Optional<LogFormat::Type>(kEfStats);
  } else if (type == "pub") {
    return Optional<LogFormat::Type>(kPub);
  } else {
    return Optional<LogFormat::Type>::Invalid();
  }
}

std::unique_ptr<LogFormat> LogFormat::Init(LogFormat::Type type) {
  switch (type) {
    case kV6:
      return V6LogFormat::Init();
    case kV6Device:
      return V6DeviceLogFormat::Init();
    case kEf:
      return EfLogFormat::Init();
    case kEfDevice:
      return EfDeviceLogFormat::Init();
    case kReport:
      return ReportLogFormat::Init();
    case kEfIc:
      return EfIcLogFormat::Init();
    case kEfImp:
      return EfImpLogFormat::Init();
    case kEfStats:
      return EfStatsLogFormat::Init();
    case kPub:
      return EfPubLogFormat::Init();
    default:
      return nullptr;
  }
}

// ------------------------------------------------------------------
// V6LogFormat

std::unique_ptr<V6LogFormat> V6LogFormat::Init() {
  return std::unique_ptr<V6LogFormat>(new V6LogFormat());
}

#define V6_SECTION_DELIMITER '\u0001'
#define V6_OPTION_DELIMITER '\u0002'

#define TIME_SECTION_INDEX 1
#define TIME_OPTION_INDEX 10

#define DEVICE_SECTION_INDEX 6
#define DEVICE_OPTION_INDEX 0

bool V6LogFormat::ExtractKeyAndTs(const char* payload, size_t len,
    std::string* key, time_t* ts) const {
  if (!payload || !key || !ts || len <= 0)
    return false;

  const char *sbegin, *send;
  if (!ExtractString(payload, payload + len, V6_SECTION_DELIMITER,
              TIME_SECTION_INDEX, &sbegin, &send)) {
    return false;
  }

  const char *obegin, *oend;
  if (!ExtractString(sbegin, send, V6_OPTION_DELIMITER,
              TIME_OPTION_INDEX, &obegin, &oend)) {
    return false;
  }

  time_t temp = atol(obegin) / 1000;
  if (temp <= 0)
    return false;

  *ts = temp;
  *key = "";
  return true;
}

bool V6LogFormat::ParseKey(const std::string& key,
    std::map<char, std::string>* m) const {
  if (!m)
    return false;

  m->clear();
  return true;
}

// ------------------------------------------------------------------
// V6DeviceLogFormat

std::unique_ptr<V6DeviceLogFormat> V6DeviceLogFormat::Init() {
  return std::unique_ptr<V6DeviceLogFormat>(new V6DeviceLogFormat());
}

bool V6DeviceLogFormat::ExtractKeyAndTs(const char* payload, size_t len,
    std::string* key, time_t* ts) const {
  if (!payload || !key || !ts || len <= 0)
    return false;

  const char *sbegin, *send;
  if (!ExtractString(payload, payload + len, V6_SECTION_DELIMITER,
              TIME_SECTION_INDEX, &sbegin, &send)) {
    return false;
  }

  const char *obegin, *oend;
  if (!ExtractString(sbegin, send, V6_OPTION_DELIMITER,
              TIME_OPTION_INDEX, &obegin, &oend)) {
    return false;
  }

  time_t temp = atol(obegin) / 1000;
  if (temp <= 0)
    return false;

  *ts = temp;

  // extract device
  if (!ExtractString(payload, payload + len, V6_SECTION_DELIMITER,
              DEVICE_SECTION_INDEX, &sbegin, &send)) {
    return false;
  }

  if (!ExtractString(sbegin, send, V6_OPTION_DELIMITER,
              DEVICE_OPTION_INDEX, &obegin, &oend)) {
    return false;
  }

  if (obegin == oend) {
    key->assign("pc");
  } else {
    if (strncmp(obegin, "pc", 2) == 0 || strncmp(obegin, "na", 2) == 0) {
      key->assign("pc");
    } else {
      key->assign("mobile");
    }
  }
  return true;
}

bool V6DeviceLogFormat::ParseKey(const std::string& key,
    std::map<char, std::string>* m) const {
  if (!m || key.empty())
    return false;

  m->clear();
  if (key == "pc" || key == "mobile") {
    (*m)['D'] = key;
  } else {
    return false;
  }
  return true;
}

// ------------------------------------------------------------------
// EfLogFormat

std::unique_ptr<EfLogFormat> EfLogFormat::Init() {
  return std::unique_ptr<EfLogFormat>(new EfLogFormat());
}

#define EF_DELIMITER '\t'
#define TIME_INDEX 6
#define DEVICE_INDEX 41
#define TIME_FORMAT "%Y%m%d%H%M"
#define TIME_LENGTH 12

bool EfLogFormat::ExtractKeyAndTs(const char* payload, size_t len,
    std::string* key, time_t* ts) const {
  if (!payload || !key || !ts || len <= 0)
    return false;

  const char *begin, *end;
  if (!ExtractString(payload, payload + len, EF_DELIMITER,
              TIME_INDEX, &begin, &end)) {
    return false;
  }

  std::string time_str(begin, TIME_LENGTH);
  time_t time_stamp = StrToTs(time_str, TIME_FORMAT);
  if (time_stamp <= 0) {
    return false;
  }

  *ts = time_stamp;
  *key = "";
  return true;
}

bool EfLogFormat::ParseKey(const std::string& key,
    std::map<char, std::string>* m) const {
 if (!m)
   return false;

 m->clear();
 return true;
}

// ------------------------------------------------------------------
// EfDeviceLogFormat

std::unique_ptr<EfDeviceLogFormat> EfDeviceLogFormat::Init() {
  return std::unique_ptr<EfDeviceLogFormat>(new EfDeviceLogFormat());
}

bool EfDeviceLogFormat::ExtractKeyAndTs(const char* payload, size_t len,
    std::string* key, time_t* ts) const {
  if (!payload || !key || !ts || len <= 0)
    return false;

  const char *begin, *end;
  if (!ExtractString(payload, payload + len, EF_DELIMITER,
              TIME_INDEX, &begin, &end)) {
    return false;
  }

  std::string time_str(begin, TIME_LENGTH);
  time_t time_stamp = StrToTs(time_str, TIME_FORMAT);
  if (time_stamp <= 0) {
    return false;
  }
  *ts = time_stamp;

  if (!ExtractString(payload, payload + len, EF_DELIMITER,
              DEVICE_INDEX, &begin, &end)) {
    return false;
  }

  if (begin == end) {
    key->assign("pc");
  } else {
    if (strncmp(begin, "General", 2) == 0 || strncmp(begin, "Na", 2) == 0) {
      key->assign("pc");
    } else {
      key->assign("mobile");
    }
  }

  return true;
}

bool EfDeviceLogFormat::ParseKey(const std::string& key,
    std::map<char, std::string>* m) const {
  if (!m || key.empty())
    return false;

  m->clear();
  if (key == "pc" || key == "mobile") {
    (*m)['D'] = key;
  } else {
    return false;
  }
  return true;
}

// ------------------------------------------------------------------
// ReportLogFormat

std::unique_ptr<ReportLogFormat> ReportLogFormat::Init() {
  return std::unique_ptr<ReportLogFormat>(new ReportLogFormat());
}

bool ReportLogFormat::ExtractKeyAndTs(const char* payload, size_t len,
    std::string* key, time_t* ts) const {
  if (!payload || !key || !ts || len <= 0)
    return false;

  std::string time_str(payload, TIME_LENGTH);
  time_t time_stamp = StrToTs(time_str, TIME_FORMAT);
  if (time_stamp <= 0) {
    return false;
  }
  *ts = time_stamp;

  return true;
}

bool ReportLogFormat::ParseKey(const std::string& key,
    std::map<char, std::string>* m) const {
  if (!m)
    return false;

  m->clear();
  return true;
}

// ------------------------------------------------------------------
// EfIcLogFormat

std::unique_ptr<EfIcLogFormat> EfIcLogFormat::Init() {
  return std::unique_ptr<EfIcLogFormat>(new EfIcLogFormat());
}

#define ACTION_INDEX 2

bool EfIcLogFormat::ExtractKeyAndTs(const char* payload, size_t len,
    std::string* key, time_t* ts) const {
  if (!payload || !key || !ts || len <= 0)
    return false;

  const char *begin, *end;
  if (!ExtractString(payload, payload + len, EF_DELIMITER,
              TIME_INDEX, &begin, &end)) {
    return false;
  }

  std::string time_str(begin, TIME_LENGTH);
  time_t time_stamp = StrToTs(time_str, TIME_FORMAT);
  if (time_stamp <= 0) {
    return false;
  }
  *ts = time_stamp;

  if (!ExtractString(payload, payload + len, EF_DELIMITER,
              ACTION_INDEX, &begin, &end)) {
    return false;
  }
  
  if (begin == end) {
    return false;
  }

  int type = atoi(begin);
  
  if (type == 2) {
    key->assign("click");
    return true;
  } else if (type != 1) {
    return false;
  }

  if (!ExtractString(payload, payload + len, EF_DELIMITER,
              DEVICE_INDEX, &begin, &end)) {
    return false;
  }

  std::string device;
  if (begin == end) {
    key->assign("imp_pc");
  } else {
    if (strncmp(begin, "General", 2) == 0 || strncmp(begin, "Na", 2) == 0) {
      key->assign("imp_pc");
    } else {
      key->assign("imp_mobile");
    }
  }

  return true;
}

bool EfIcLogFormat::ParseKey(const std::string& key,
    std::map<char, std::string>* m) const {
  if (key.empty() || !m)
    return false;

  if (key == "click") {
    (*m)['D'] = "";
    (*m)['A'] = "click";
  } else if (key == "imp_pc") {
    (*m)['D'] = "pc";
    (*m)['A'] = "imp";
  } else if (key == "imp_mobile") {
    (*m)['D'] = "mobile";
    (*m)['A'] = "imp";
  } else {
    return false;
  }

  return true;
}

// ------------------------------------------------------------------
// EfImpLogFormat

std::unique_ptr<EfImpLogFormat> EfImpLogFormat::Init() {
  return std::unique_ptr<EfImpLogFormat>(new EfImpLogFormat());
}


bool EfImpLogFormat::ExtractKeyAndTs(const char* payload, size_t len,
    std::string* key, time_t* ts) const {
  if (!payload || !key || !ts || len <= 0)
    return false;

  const char *begin, *end;
  if (!ExtractString(payload, payload + len, EF_DELIMITER,
              TIME_INDEX, &begin, &end)) {
    return false;
  }

  std::string time_str(begin, TIME_LENGTH);
  time_t time_stamp = StrToTs(time_str, TIME_FORMAT);
  if (time_stamp <= 0) {
    return false;
  }
  *ts = time_stamp;

  if (!ExtractString(payload, payload + len, EF_DELIMITER,
              ACTION_INDEX, &begin, &end)) {
    return false;
  }
  
  if (begin == end) {
    return false;
  }

  int type = atoi(begin);
  
  if (type == 1) {
    key->assign("imp");
  } else if (type == 2) {
    key->assign("click");
  } else {
    return false;
  }

  return true;
}

bool EfImpLogFormat::ParseKey(const std::string& key,
    std::map<char, std::string>* m) const {
  if (key.empty() || !m)
    return false;

  if (key == "click") {
    (*m)['A'] = "click";
  } else if (key == "imp") {
    (*m)['A'] = "imp";
  } else {
    return false;
  }

  return true;
}

// ------------------------------------------------------------------
// EfStatsLogFormat

std::unique_ptr<EfStatsLogFormat> EfStatsLogFormat::Init() {
  return std::unique_ptr<EfStatsLogFormat>(new EfStatsLogFormat());
}

bool EfStatsLogFormat::ExtractKeyAndTs(const char* payload, size_t len,
    std::string* key, time_t* ts) const {
  if (!payload || !key || !ts || len <= 0)
    return false;

  const char *begin, *end;
  if (!ExtractString(payload, payload + len, EF_DELIMITER,
              TIME_INDEX, &begin, &end)) {
    return false;
  }

  std::string time_str(begin, TIME_LENGTH);
  time_t time_stamp = StrToTs(time_str, TIME_FORMAT);
  if (time_stamp <= 0) {
    return false;
  }
  *ts = time_stamp;

  if (!ExtractString(payload, payload + len, EF_DELIMITER,
              ACTION_INDEX, &begin, &end)) {
    return false;
  }
  
  if (begin == end) {
    return false;
  }

  int type = atoi(begin);
  
  if (type == 11) {
    key->assign("imp");
  } else if (type == 12) {
    key->assign("click");
  } else {
    return false;
  }

  return true;
}

bool EfStatsLogFormat::ParseKey(const std::string& key,
    std::map<char, std::string>* m) const {
  if (key.empty() || !m)
    return false;

  if (key == "click") {
    (*m)['A'] = "click";
    (*m)['p'] = "click";
  } else if (key == "imp") {
    (*m)['A'] = "imp";
    (*m)['p'] = "impression";
  } else {
    return false;
  }

  return true;
}

// ------------------------------------------------------------------
// EfPubLogFormat

std::unique_ptr<EfPubLogFormat> EfPubLogFormat::Init() {
  return std::unique_ptr<EfPubLogFormat>(new EfPubLogFormat());
}

#define TIME_INDEX_PUB 11

bool EfPubLogFormat::ExtractKeyAndTs(const char* payload, size_t len,
    std::string* key, time_t* ts) const {
  if (!payload || !key || !ts || len <= 0)
    return false;

  const char *begin, *end;
  if (!ExtractString(payload, payload + len, EF_DELIMITER,
              TIME_INDEX_PUB, &begin, &end)) {
    return false;
  }

  std::string time_str(begin, TIME_LENGTH);
  time_t time_stamp = StrToTs(time_str, TIME_FORMAT);
  if (time_stamp <= 0) {
    return false;
  }

  *ts = time_stamp;
  *key = "";
  return true;
}

bool EfPubLogFormat::ParseKey(const std::string& key,
    std::map<char, std::string>* m) const {
  if (!m)
    return false;

  m->clear();
  return true;
}

}   // namespace log2hdfs
