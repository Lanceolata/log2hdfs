
#include <sstream>

//#include "log2hdfs/util/tools.h"
#include "tools.h"

namespace log2hdfs {

namespace util {

void split(const std::string& str, char separator,
           std::vector<std::string>& vec) {
  if (str.empty())
      return;

  std::stringstream ss(str);
  std::string segment;

  while(std::getline(ss, segment, separator)) {
    vec.push_back(segment);
  }
}

#define SPACE_STR " \t\n\r"

std::string& ltrim(std::string& str) {
    if (str.empty())
        return str;

    return str.erase(0, str.find_first_not_of(SPACE_STR));
}

std::string& rtrim(std::string& str) {
  if (str.empty()) 
      return str;

  return str.erase(str.find_last_not_of(SPACE_STR) + 1);
}

std::string& trim(std::string& str) {
  return ltrim(rtrim(str));
}

#define COMMENT_STR "#"

std::string& remove_comments(std::string& str) {
  if (str.empty())
      return str;

  std::size_t pos = str.find(COMMENT_STR);
  if (pos != std::string::npos) {
    str.erase(pos);
  }
  return str;
}

}   // namespace util

}   // namespace log2hdfs

