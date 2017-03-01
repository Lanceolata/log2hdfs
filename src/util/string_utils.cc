// Copyright (c) 2017 Lanceolata

#include "util/string_utils.h"
#include <string.h>
#include <sstream>

namespace log2hdfs {

const char white_chars[] = " \f\n\r\t\v";
const char comment_str[] = "#";

std::vector<std::string> SplitString(const std::string &input,
                                     const std::string &delimiters,
                                     WhitespaceHandling whitespace,
                                     SplitResult type) {
  std::vector<std::string> result;
  if (input.empty()) {
    return result;
  }

  std::string::size_type start = 0;
  while (start != std::string::npos) {
    auto end = input.find_first_of(delimiters, start);

    std::string piece;
    if (end == std::string::npos) {
      piece = input.substr(start);
      start = std::string::npos;
    } else {
      piece = input.substr(start, end - start);
      start = end + 1;
    }

    if (whitespace == kTrimWhitespace) {
      piece = TrimString(piece, white_chars);
    }

    if (type == kSplitAll || !piece.empty()) {
      result.push_back(piece);
    }
  }

  return result;
}

std::string TrimString(const std::string &input) {
  return TrimString(input, white_chars);
}

std::string TrimString(const std::string &input,
                       const std::string &trim_chars) {
  auto begin = input.find_first_not_of(trim_chars);
  if (begin == std::string::npos) {
    return "";
  }

  auto end = input.find_last_not_of(trim_chars);
  if (end == std::string::npos) {
    return input.substr(begin);
  }

  return input.substr(begin, end - begin + 1);
}

std::string RemoveComments(const std::string &input) {
  return RemoveComments(input, comment_str);
}

std::string RemoveComments(const std::string &input,
                           const std::string &comment) {
  auto end = input.find(comment);
  if (end != std::string::npos) {
    if (end == 0) {
      return "";
    }
    return input.substr(0, end - 1);
  }
  return input;
}

bool StartsWith(const std::string &str, const std::string &prefix) {
  return StartsWith(str.c_str(), prefix.c_str());
}

bool StartsWith(const std::string &str, const char *prefix) {
  return StartsWith(str.c_str(), prefix);
}

bool StartsWith(const char *str, const char *prefix) {
  return strncmp(str, prefix, strlen(prefix)) == 0;
}

bool EndsWith(const std::string &str, const std::string &suffix) {
  return EndsWith(str.c_str(), suffix.c_str());
}

bool EndsWith(const std::string &str, const char *suffix) {
  return EndsWith(str.c_str(), suffix);
}

bool EndsWith(const char *str, const char *suffix) {
  const auto len1 = strlen(str);
  const auto len2 = strlen(suffix);
  if (len2 > len1)
    return false;
  const char *end = str + len1 - len2;
  return memcmp(end, suffix, len2) == 0;
}

std::string TsToString(time_t ts) {
  return std::to_string(ts);
}

time_t StringToTs(const std::string &str) {
  time_t ts = atol(str.c_str());
  return ts;
}

}   // namespace log2hdfs
