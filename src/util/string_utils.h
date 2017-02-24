// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_UTIL_STRING_UTILS_H_
#define LOG2HDFS_UTIL_STRING_UTILS_H_

#include <string>
#include <vector>

namespace log2hdfs {

enum WhitespaceHandling {
  kKeepWhitespace,
  kTrimWhitespace
};

enum SplitResult {
  kSplitAll,
  kSplitNonempty
};

extern std::vector<std::string> SplitString(const std::string &input,
                                            const std::string &delimiters,
                                            WhitespaceHandling whitespace,
                                            SplitResult type);

extern std::string TrimString(const std::string &input);

extern std::string TrimString(const std::string &input,
                              const std::string &trim_chars);

extern std::string RemoveComments(const std::string &input);

extern std::string RemoveComments(const std::string &input,
                                  const std::string &comment);

extern bool StartsWith(const std::string &str, const std::string &prefix);

extern bool StartsWith(const std::string &str, const char *prefix);

extern bool StartsWith(const char *str, const char *prefix);

extern bool EndsWith(const std::string &str, const std::string &suffix);

extern bool EndsWith(const std::string &str, const char *suffix);

extern bool EndsWith(const char *str, const char *suffix);

extern std::string TsToString(time_t ts);

extern time_t StringToTs(const std::string &str);

}   // namespace log2hdfs

#endif  // LOG2HDFS_UTIL_STRING_UTILS_H_
