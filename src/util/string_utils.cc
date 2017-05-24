// Copyright (c) 2017 Lanceolata

#include "util/string_utils.h"
#include <string.h>
#include <sstream>

const char white_chars[] = " \f\n\r\t\v";
const char comment_str[] = "#";

std::vector<std::string> SplitString(
    const std::string& input, const std::string& delimiters,
    WhitespaceHandling whitespace, SplitResult type) {
  std::vector<std::string> res;
  if (input.empty())
    return res;

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

    if (whitespace == WhitespaceHandling::kTrimWhitespace)
      piece = TrimString(piece, white_chars);

    if (type == SplitResult::kSplitAll || !piece.empty())
      res.push_back(piece);
  }
  return res;
}

std::string TrimString(const std::string& input) {
  return TrimString(input, white_chars);
}

std::string TrimString(const std::string& input,
                       const std::string& trim_chars) {
  auto begin = input.find_first_not_of(trim_chars);
  if (begin == std::string::npos)
    return "";

  auto end = input.find_last_not_of(trim_chars);
  if (end == std::string::npos)
    return input.substr(begin);

  return input.substr(begin, end - begin + 1);
}

std::string RemoveComments(const std::string& input) {
  return RemoveComments(input, comment_str);
}

std::string RemoveComments(const std::string& input,
                           const std::string& comment) {
  auto end = input.find(comment);
  if (end != std::string::npos) {
    if (end == 0)
      return "";

    return input.substr(0, end - 1);
  }
  return input;
}

bool StartsWith(const std::string& input, const std::string& prefix) {
  return strncmp(input.c_str(), prefix.c_str(), prefix.size()) == 0;
}

bool EndsWith(const std::string& input, const std::string& suffix) {
  const auto len1 = input.size();
  const auto len2 = suffix.size();

  if (len2 > len1)
    return false;

  const char *end = input.c_str() + len1 - len2;
  return memcmp(end, suffix.c_str(), len2) == 0;
}
