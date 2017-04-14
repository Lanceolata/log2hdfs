// Copyright (c) 2017 Lanceolata

#include "kafka2hdfs/log_format_impl.h"
#include <string.h>

namespace log2hdfs {

// ------------------------------------------------------------------
// LogFormat

Optional<LogFormat::Type> LogFormat::GetTypeFromString(
    const std::string &type) {
  if (type == "v6log") {
    return Optional<LogFormat::Type>(LogFormat::Type::kV6Log);
  } else if (type == "eflog") {
    return Optional<LogFormat::Type>(LogFormat::Type::kEfLog);
  } else {
    return Optional<LogFormat::Type>::Invalid();
  }
}

std::shared_ptr<LogFormat> LogFormat::Init(LogFormat::Type type) {
  std::shared_ptr<LogFormat> res;
  switch (type) {
    case kV6Log:
      res = V6LogFormat::Init();
      break;
    case kEfLog:
      res = nullptr;
      break;
  }
  return res;
}

// ------------------------------------------------------------------
// V6LogFormat

#define V6_SECTION_DELIMITER '\u0001'
#define V6_OPTION_DELIMITER '\u0002'

#define TIME_SECTION_INDEX 1
#define TIME_OPTION_INDEX 10

#define DEVICE_SECTION_INDEX 6
#define DEVICE_OPTION_INDEX 0

bool V6LogFormat::ExtractInfoFromPayload(
    const char *payload,
    time_t *ts,
    std::string *device,
    std::string *type) const {
  if (!payload || !ts || !device || !type) {
    return false;
  }
  (*type).clear();
  int n = 0;
  const char *temp = payload, *p = NULL;
  const char *index1 = NULL, *index2 = NULL;

  while (temp != NULL && (p = strchr(temp, V6_SECTION_DELIMITER)) != NULL) {
    ++n;
    if (n == TIME_SECTION_INDEX) {
      if (*(p + 1) == V6_SECTION_DELIMITER) return false;
      index1 = p + 1;
    } else if (n == DEVICE_SECTION_INDEX) {
      if (*(p + 1) == V6_SECTION_DELIMITER) return false;
      index2 = p + 1;
      break;
    }
    temp = p + 1;
  }

  if (index1 == NULL || index2 == NULL) return false;

  n = 0;
  temp = index1;

  while (temp != NULL && (p = strchr(temp, V6_OPTION_DELIMITER)) != NULL) {
    ++n;
    if (n == TIME_OPTION_INDEX) {
      if (*(p + 1) == V6_OPTION_DELIMITER) return false;
      *ts = atol(p + 1) / 1000;
      break;
    }
    temp = index1 + 1;
  }

  if (temp == NULL || p == NULL) return false;

  temp = index2;
  if (*temp == V6_OPTION_DELIMITER) {
    *device = "pc";
  } else {
    if (strncmp(temp, "pc", 2) == 0 || strncmp(temp, "na", 2) == 0) {
      *device = "pc";
    } else {
      *device = "mobile";
    }
  }
  return true;
}

}   // namespace log2hdfs
