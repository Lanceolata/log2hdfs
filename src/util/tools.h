// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_UTIL_TOOLS_H
#define LOG2HDFS_UTIL_TOOLS_H

#include <string>
#include <vector>

namespace log2hdfs {

namespace util {

extern void Split(const std::string& str, char separator,
                  std::vector<std::string> *vec);

extern void Ltrim(std::string *str);

extern void Rtrim(std::string *str);

extern void Trim(std::string *str);

extern void RemoveComments(std::string *str);

}   // namespace util

}   // namespace log2hdfs

#endif  // LOG2HDFS_UTIL_TOOLS_H
