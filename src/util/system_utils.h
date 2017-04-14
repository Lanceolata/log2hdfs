// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_UTIL_SYSTEM_UTILS_H_
#define LOG2HDFS_UTIL_SYSTEM_UTILS_H_

#include <dirent.h>
#include <string>
#include <vector>
#include "util/optional.h"

namespace log2hdfs {

extern bool IsFile(const std::string &path);

extern bool IsFile(const char *path);

extern bool IsDir(const std::string &path);

extern bool IsDir(const char *path);

extern bool MakeDir(const std::string &path);

extern bool MakeDir(const char *path);

extern bool Rename(const std::string &oldpath, const std::string &newpath);

extern bool Rename(const char *oldpath, const char *newpath);

extern bool RmFile(const std::string &path);

extern bool RmFile(const char *path);

extern Optional<off_t> FileSize(const std::string &path);

extern Optional<off_t> FileSize(const char *path);

extern Optional<time_t> FileMtime(const std::string &path);

extern Optional<time_t> FileMtime(const char *path);

extern Optional<std::string> BaseName(const std::string &filepath);

extern Optional<std::string> BaseName(const char *filepath);

extern Optional<time_t> StrToTs(const std::string &str,
                                const std::string &format);

extern Optional<time_t> StrToTs(const char *str,
                                const char *format);

extern Optional<std::vector<std::string> > ScanDirFile(
    const std::string &path,
    int (*filter)(const struct dirent *),
    int (*compar)(const struct dirent **, const struct dirent **));

extern Optional<std::vector<std::string> > ScanDirFile(
    const char *path,
    int (*filter)(const struct dirent *),
    int (*compar)(const struct dirent **, const struct dirent **));

bool ExecuteCommand(const std::string &command, std::string *error);

bool ExecuteCommand(const char *command, std::string *error);

}   // namespace log2hdfs

#endif  // LOG2HDFS_UTIL_SYSTEM_UTILS_H_
