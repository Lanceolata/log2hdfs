// Copyright (c) 2017 Lanceolata

// #include "util/tools.h"
#include "tools.h"

#include <sstream>

namespace log2hdfs {

namespace util {

void Split(const std::string& str, char separator,
           std::vector<std::string> *vec) {
  if (str.empty() || vec == NULL)
      return;

  std::stringstream ss(str);
  std::string segment;
  (*vec).clear();

  while (std::getline(ss, segment, separator)) {
    (*vec).push_back(segment);
  }
}

#define SPACE_STR " \t\n\r"

void Ltrim(std::string *str) {
  if (str != NULL && !(*str).empty()) {
    (*str).erase(0, (*str).find_first_not_of(SPACE_STR));
  }
}

void Rtrim(std::string *str) {
  if (str != NULL && !(*str).empty()) {
    (*str).erase((*str).find_last_not_of(SPACE_STR) + 1);
  }
}

void Trim(std::string *str) {
  Rtrim(str);
  Ltrim(str);
}

#define COMMENT_STR "#"

void RemoveComments(std::string *str) {
  if (str != NULL && !(*str).empty()) {
    std::size_t pos = (*str).find(COMMENT_STR);
    if (pos != std::string::npos) {
      (*str).erase(pos);
    }
  }
}

}   // namespace util

}   // namespace log2hdfs

