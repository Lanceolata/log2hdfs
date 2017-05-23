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

/**
 * Get file size.
 * 
 * @return On success, file size is returned. On error, -1 is returned,
 * and errno is set appropriately.
 */
extern off_t FileSize(const std::string& path);

extern time_t FileAtime(const std::string& path);

// Get file st_mtime.
// return:
//   On success, file mtime is returned. On error, -1 is returned,
//   and errno is set appropriately.
extern time_t FileMtime(const std::string& path);

// Get file st_ctim.
// return:
//   On success, true is returned, and ts is set to file st_ctim.
//   On error, false is returned, and errno is set appropriately.
extern bool FileCtim(const std::string& path, struct timespec* ts);

extern std::string BaseName(const std::string& path);

extern std::string DirName(const std::string& path);

extern bool IsDir(const std::string& path);

extern bool MakeDir(const std::string& path);

extern std::string NormalDirPath(const std::string& path);

// Scandir and return file names in dir.
// return:
//   On success, true is returned, clear names and file name push into names.
//   On error, false is returned, and errno is set appropriately.
extern bool ScanDir(
    const std::string& path,
    int (*filter)(const struct dirent*),
    int (*compar)(const struct dirent**, const struct dirent**),
    std::vector<std::string>* names);

extern bool Rename(const std::string& oldpath, const std::string& newpath);

// Convert a string representation of time to a time_t.
// return:
//   On success, time_t is returned.
//   On error, -1 is returned.
extern time_t StrToTs(const std::string& str, const char* format);

bool ExecuteCommand(const std::string& command, std::string* error);

}   // namespace log2hdfs

#endif  // LOG2HDFS_UTIL_SYSTEM_UTILS_H_
