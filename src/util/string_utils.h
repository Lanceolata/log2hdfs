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

/*
 * 分割字符串
 *  - input         输入字符串
 *  - delimiters    分隔符
 *  - whitespace    空白字符处理
 *  - type          是否去除空项
 * 
 * */
extern std::vector<std::string> SplitString(const std::string &input,
                                            const std::string &delimiters,
                                            WhitespaceHandling whitespace,
                                            SplitResult type);

// 去除收尾空白字符(" \f\n\r\t\v")
extern std::string TrimString(const std::string &input);

extern std::string TrimString(const std::string &input,
                              const std::string &trim_chars);

// 去掉注释(#之后的字符)
extern std::string RemoveComments(const std::string &input);

extern std::string RemoveComments(const std::string &input,
                                  const std::string &comment);

extern bool StartsWith(const std::string &str, const std::string &prefix);

extern bool StartsWith(const std::string &str, const char *prefix);

extern bool StartsWith(const char *str, const char *prefix);

extern bool EndsWith(const std::string &str, const std::string &suffix);

extern bool EndsWith(const std::string &str, const char *suffix);

extern bool EndsWith(const char *str, const char *suffix);

}   // namespace log2hdfs

#endif  // LOG2HDFS_UTIL_STRING_UTILS_H_
