// Copyright (c) 2017 Lanceolata

#ifndef LOG2HDFS_UTIL_SYSTEM_UTILS_H_
#define LOG2HDFS_UTIL_SYSTEM_UTILS_H_

#include <dirent.h>
#include <string>
#include <vector>
#include "util/optional.h"

/**
 * Whether path is file
 * 
 * @returns True If path is file; false otherwise.
 */
bool IsFile(const std::string& path);

/**
 * Remove file
 * 
 * @returns True if unlink path success, false otherwise.
 */
extern bool RmFile(const std::string& path);

/**
 * Get file size
 * 
 * @returns On success, file zise is returned. On error, -1 is returned,
 *          and errno is set appropriately.
 */
extern off_t FileSize(const std::string& path);

/**
 * Get file st_atime
 * 
 * @returns On success, file st_atime is returned. On error, -1 is returned,
 *          and errno is set appropriately.
 */
extern time_t FileAtime(const std::string& path);

/**
 * Get file st_mtime
 * 
 * @returns On success, file st_mtime is returned. On error, -1 is returned,
 *          and errno is set appropriately.
 */
extern time_t FileMtime(const std::string& path);

/**
 * Get file st_ctim
 * 
 * @returns On success, true is returned, and ts is set to file st_ctim.
 *          On error, false is returned, and errno is set appropriately.
 */
extern bool FileCtim(const std::string& path, struct timespec* ts);

/**
 * Parse file base name
 * 
 * @returns On success, base name is returned. On error, empty string
 *          is returned.
 */
extern std::string BaseName(const std::string& path);

/**
 * Parse file dir name
 * 
 * @returns On success, dir name is returned. On error, empty string
 *          is returned.
 */
extern std::string DirName(const std::string& path);

/**
 * Whether path is dir
 * 
 * @returns True if path is dir; false otherwise.
 */
extern bool IsDir(const std::string& path);

/**
 * Make directory
 * 
 * @returns True if directory exists or make directory success;
 *          false otherwise.
 */
extern bool MakeDir(const std::string& path);

/**
 * Normalize directory path
 * 
 * @returns On success, normal path is returned. On error,
 *          empty string is returned.
 */
extern std::string NormalDirPath(const std::string& path);

/**
 * Scandir and return names in dir
 * 
 * @returns On success, true is returned, names push into vector names.
 *          On error, false is returned, and errno is set appropriately.
 */
extern bool ScanDir(
    const std::string& path,
    int (*filter)(const struct dirent*),
    int (*compar)(const struct dirent**, const struct dirent**),
    std::vector<std::string>* names);

/**
 * Rename old path to new path
 * 
 * @returns On success, true is returned. On error, false is returned,
 *          and errno is set appropriately.
 */
extern bool Rename(const std::string& oldpath, const std::string& newpath);

/**
 * Convert a string representation of time to a time_t
 * 
 * @returns On success, time_t is returned. On error, -1 is returned.
 */
extern time_t StrToTs(const std::string& str, const char* format);

/**
 * Execute Command
 * 
 * @returns On success, true is returned. On error, false is returned,
 *          and errstr is set appropriately.
 */
bool ExecuteCommand(const std::string& command, std::string* errstr);

#endif  // LOG2HDFS_UTIL_SYSTEM_UTILS_H_
