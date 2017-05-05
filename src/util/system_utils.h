// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_UTIL_SYSTEM_UTILS_H_
#define LOG2HDFS_UTIL_SYSTEM_UTILS_H_

#include <dirent.h>
#include <string>
#include <vector>
#include "util/optional.h"

namespace log2hdfs {

extern bool IsFile(const std::string& path);

extern bool RmFile(const std::string& path);

extern Optional<off_t> FileSize(const std::string& path);

extern Optional<time_t> FileMtime(const std::string& path);

extern Optional<struct timespec> FileCtime(const std::string& path);

extern Optional<std::string> BaseName(const std::string& path);

extern Optional<std::string> DirName(const std::string& path);

extern bool IsDir(const std::string& path);

extern bool MakeDir(const std::string& path);

extern std::string NormalDirPath(const std::string& path);

extern Optional<std::vector<std::string>> ScanDir(
    const std::string& path,
    int (*filter)(const struct dirent*),
    int (*compar)(const struct dirent**, const struct dirent**));

extern bool Rename(const std::string& oldpath, const std::string& newpath);

extern Optional<time_t> StrToTs(const std::string& str, const char* format);

bool ExecuteCommand(const std::string& command, std::string* error);

}   // namespace log2hdfs

#endif  // LOG2HDFS_UTIL_SYSTEM_UTILS_H_
