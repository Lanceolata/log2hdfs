// Copyright (c) 2017 Lanceolata

#include "util/system_utils.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <utility>
#include <memory>

namespace log2hdfs {

bool IsFile(const std::string &path) {
  if (path.empty()) {
    return false;
  }
  return IsFile(path.c_str());
}

bool IsFile(const char *path) {
  if (!path) {
    return false;
  }
  struct stat stbuf;
  if (stat(path, &stbuf) != 0) {
    return false;
  }
  return S_ISREG(stbuf.st_mode);
}

bool IsDir(const std::string &path) {
  if (path.empty()) {
    return false;
  }
  return IsDir(path.c_str());
}

bool IsDir(const char *path) {
  if (!path) {
    return false;
  }
  struct stat stbuf;
  if (stat(path, &stbuf) != 0) {
    return false;
  }
  return S_ISDIR(stbuf.st_mode);
}

#define DIR_MODE (S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH)

bool MakeDir(const std::string &path) {
  if (path.empty()) {
    return false;
  }
  return MakeDir(path.c_str());
}

bool MakeDir(const char *path) {
  if (!path) {
    return false;
  }
  if (access(path, F_OK) < 0) {
    if (mkdir(path, DIR_MODE) < 0) {
      return false;
    }
  }
  return true;
}

bool Rename(const std::string &oldpath, const std::string &newpath) {
  if (oldpath.empty() || newpath.empty()) {
    return false;
  }
  return Rename(oldpath.c_str(), newpath.c_str());
}

bool Rename(const char *oldpath, const char *newpath) {
  if (!oldpath || !newpath) {
    return false;
  }
  return rename(oldpath, newpath) == 0;
}

bool RmFile(const std::string &path) {
  if (path.empty()) {
    return false;
  }
  return RmFile(path.c_str());
}

bool RmFile(const char *path) {
  if (!path) {
    return false;
  }
  return unlink(path) == 0;
}

Optional<off_t> FileSize(const std::string &path) {
  if (path.empty()) {
    return Optional<off_t>::Invalid();
  }
  return FileSize(path.c_str());
}

Optional<off_t> FileSize(const char *path) {
  if (!path) {
    return Optional<off_t>::Invalid();
  }
  struct stat stbuf;
  if (stat(path, &stbuf) != 0) {
    return Optional<off_t>::Invalid();
  }
  return Optional<off_t>(stbuf.st_size);
}

Optional<time_t> FileMtime(const std::string &path) {
  if (path.empty()) {
    return Optional<time_t>::Invalid();
  }
  return FileMtime(path.c_str());
}

Optional<time_t> FileMtime(const char *path) {
  if (!path) {
    return Optional<time_t>::Invalid();
  }
  struct stat stbuf;
  if (stat(path, &stbuf) != 0) {
    return Optional<time_t>::Invalid();
  }
  return Optional<time_t>(stbuf.st_mtime);
}

Optional<std::string> BaseName(const std::string &filepath) {
  if (filepath.empty()) {
    return Optional<std::string>::Invalid();
  }
  return BaseName(filepath.c_str());
}

Optional<std::string> BaseName(const char *filepath) {
  if (!filepath) {
    return Optional<std::string>::Invalid();
  }
  const char *p = strrchr(filepath, '/');
  if (p == NULL) {
    p = filepath;
  }
  return Optional<std::string>(p);
}

Optional<time_t> StrToTs(const std::string &str, const std::string &format) {
  if (str.empty() || format.empty()) {
    return Optional<time_t>::Invalid();
  }
  return StrToTs(str.c_str(), format.c_str());
}

Optional<time_t> StrToTs(const char *str, const char *format) {
  if (!str || !format) {
    return Optional<time_t>::Invalid();
  }

  struct tm timeinfo;
  memset(&timeinfo, 0, sizeof(struct tm));
  if (strptime(str, format, &timeinfo) == NULL) {
    return Optional<time_t>::Invalid();
  }
  return Optional<time_t>(mktime(&timeinfo));
}

Optional<std::vector<std::string> > ScanDirFile(
    const std::string &path,
    int (*filter)(const struct dirent *),
    int (*compar)(const struct dirent **, const struct dirent **)) {
  if (path.empty()) {
    return Optional<std::vector<std::string> >::Invalid();
  }
  return std::move(ScanDirFile(path.c_str(), filter, compar));
}

Optional<std::vector<std::string> > ScanDirFile(
    const char *path,
    int (*filter)(const struct dirent *),
    int (*compar)(const struct dirent **, const struct dirent **)) {
  if (!path) {
    return Optional<std::vector<std::string> >::Invalid();
  }
  std::vector<std::string> res;
  struct dirent **namelist = NULL;
  int num = scandir(path, &namelist, filter, compar);
  if (num == -1) {
    return Optional<std::vector<std::string> >::Invalid();
  }

  size_t size = strlen(path) + 256;
  std::unique_ptr<char[]> dirpath(new char[size]);
  char *p = dirpath.get();
  int n = snprintf(p, size, "%s/", path);

  for (int i = 0; i < num; ++i) {
    struct dirent *dep = namelist[i];
    snprintf(p + n, size - n, "%s", dep->d_name);
    if (IsFile(p)) {
      res.push_back(dep->d_name);
    }
    free(dep);
  }
  free(namelist);
  return std::move(Optional<std::vector<std::string> >(res));
}

bool ExecuteCommand(const std::string &command, std::string *error) {
  if (command.empty()) {
    if (error) {
      *error = "command empty";
    }
    return false;
  }
  return ExecuteCommand(command.c_str(), error);
}

bool ExecuteCommand(const char *command, std::string *error) {
  if (!command) {
    if (error) {
      *error = "command empty";
    }
    return false;
  }
  char buf[1024];
  bool res = true;
  int status = system(command);
  if (status < 0) {
    snprintf(buf, sizeof(buf), "system[%s] failed with errno[%d]",
             command, errno);
    res = false;
  } else {
    if (WIFEXITED(status)) {
      if ((status = WEXITSTATUS(status)) != 0) {
        snprintf(buf, sizeof(buf), "cmd[%s] exited with errcode[%d]",
                 command, status);
        res = false;
      }
    } else if (WIFSIGNALED(status)) {
      snprintf(buf, sizeof(buf), "cmd[%s] exited with signal[%d]",
               command, WTERMSIG(status));
      res = false;
    } else if (WIFSTOPPED(status)) {
      snprintf(buf, sizeof(buf), "cmd[%s] stopped with signal[%d]",
               command, WSTOPSIG(status));
      res = false;
    } else {
      snprintf(buf, sizeof(buf), "cmd[%s] exited abnormally", command);
      res = false;
    }
  }
  if (!res && error) {
    *error = buf;
  }
  return res;
}

}   // namespace log2hdfs
