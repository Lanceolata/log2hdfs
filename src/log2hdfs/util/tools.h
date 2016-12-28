#ifndef LOG2HDFS_UTIL_TOOLS_H
#define LOG2HDFS_UTIL_TOOLS_H

#include <string>
#include <vector>

namespace log2hdfs {

namespace util {

extern void split(const std::string& str, char separator,
                  std::vector<std::string>& vec);

extern std::string& ltrim(std::string& str);

extern std::string& rtrim(std::string& str);

extern std::string& trim(std::string& str);

extern std::string& remove_comments(std::string& str); 

}   // namespace util

}   // namespace log2hdfs

#endif  // LOG2HDFS_UTIL_TOOLS_H
