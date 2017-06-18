// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_UTIL_STRING_UTILS_H_
#define LOG2HDFS_UTIL_STRING_UTILS_H_

#include <string>
#include <vector>

namespace log2hdfs {

/**
 * SplitString white space handle
 */
enum WhitespaceHandling {
  kKeepWhitespace,  /**< keep white space */
  kTrimWhitespace   /**< trim white space */
};

/**
 * SplitString empty string handle
 */
enum SplitResult {
  kSplitAll,        /**< keep empty string */
  kSplitNonempty    /**< except empty string */
};

/**
 * Split string
 * 
 * @param input                 string to split
 * @param delimiters            delimiters
 * @param whitespace            white space handle
 * @param type                  empty string handle
 * 
 * @returns string vector
 */
extern std::vector<std::string> SplitString(
    const std::string& input, const std::string& delimiters,
    WhitespaceHandling whitespace, SplitResult type);

/**
 * Trim string
 * 
 * @param input                 string to trim
 * 
 * @returns string after trim
 */
extern std::string TrimString(const std::string& input);

/**
 * Trim string
 * 
 * @param input                 string to trim
 * @param trim_chars            trim chars
 * 
 * @returns string after trim
 */
extern std::string TrimString(const std::string& input,
                              const std::string& trim_chars);

/**
 * Remove comments in string
 * 
 * @param input                 string to remove comments
 * 
 * @returns string after remove comments
 */
extern std::string RemoveComments(const std::string& input);

/**
 * Remove comments in string
 * 
 * @param inuput                string to remove comments
 * @param comment               commetns start with
 * 
 * @returns string after remove comments
 */
extern std::string RemoveComments(const std::string& input,
                                  const std::string& comment);

/**
 * Whether string start with prefix
 * 
 * @param input                 input string
 * @param prefix                prefix
 * 
 * @returns true if input start with prefix; false otherwise.
 */
extern bool StartsWith(const std::string& input, const std::string& prefix);

/**
 * Whether string end with suffix
 * 
 * @param input                 input string
 * @param suffix                suffix
 * 
 * @returns true if input end with suffix; false otherwise.
 */
extern bool EndsWith(const std::string& input, const std::string& suffix);

}   // namespace log2hdfs

#endif  // LOG2HDFS_UTIL_STRING_UTILS_H_
