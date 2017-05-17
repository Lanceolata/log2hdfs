// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_UTIL_STRING_UTILS_H_
#define LOG2HDFS_UTIL_STRING_UTILS_H_

#include <string>
#include <vector>

namespace log2hdfs {

/**
 * SplitString white space handle.
 */
enum WhitespaceHandling {
  kKeepWhitespace,  /**< kepp white space. */
  kTrimWhitespace   /**< trim white space. */
};

/**
 * SplitString empty string handle.
 */
enum SplitResult {
  kSplitAll,        /**< keep empty string. */
  kSplitNonempty    /**< except empty string. */
};

/**
 * Split string.
 * 
 * @param input         string to split.
 * @param delimiters    delimiters.
 * @param whitespace    white space handle.
 * @param type          empty string handle.
 * 
 * @return              string vector.
 */
extern std::vector<std::string> SplitString(
    const std::string& input, const std::string& delimiters,
    WhitespaceHandling whitespace, SplitResult type);

extern std::string TrimString(const std::string& input);

extern std::string TrimString(const std::string& input,
                              const std::string& trim_chars);

extern std::string RemoveComments(const std::string& input);

extern std::string RemoveComments(const std::string& input,
                                  const std::string& comment);

extern bool StartsWith(const std::string& str, const std::string& prefix);

extern bool StartsWith(const std::string& str, const char* prefix);

extern bool StartsWith(const char* str, const char* prefix);

extern bool EndsWith(const std::string& str, const std::string& suffix);

extern bool EndsWith(const std::string& str, const char* suffix);

extern bool EndsWith(const char* str, const char* suffix);

}   // namespace log2hdfs

#endif  // LOG2HDFS_UTIL_STRING_UTILS_H_
