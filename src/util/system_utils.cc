// Copyright (c) 2017 Lanceolata

#include "util/system_utils.h"
#include <time.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>

namespace log2hdfs {

bool IsFile(const std::string& path) {
  if (path.empty())
    return false;

  struct stat stbuf;
  if (stat(path.c_str(), &stbuf) != 0)
    return false;

  return S_ISREG(stbuf.st_mode);
}

bool RmFile(const std::string& path) {
  if (path.empty())
    return false;

  return unlink(path.c_str()) == 0;
}

Optional<off_t> FileSize(const std::string& path) {
  if (path.empty())
    return Optional<off_t>::Invalid();

  struct stat stbuf;
  if (stat(path.c_str(), &stbuf) != 0)
    return Optional<off_t>::Invalid();

  return Optional<off_t>(stbuf.st_size);
}

Optional<time_t> FileMtime(const std::string& path) {
  if (path.empty())
    return Optional<time_t>::Invalid();

  struct stat stbuf;
  if (stat(path.c_str(), &stbuf) != 0)
    return Optional<time_t>::Invalid();

  return Optional<time_t>(stbuf.st_mtime);
}

Optional<struct timespec> FileCtime(const std::string& path) {
  if (path.empty())
    return Optional<timespec>::Invalid();

  struct stat stbuf;
  if (stat(path.c_str(), &stbuf) != 0)
    return Optional<timespec>::Invalid();

  struct timespec ts = stbuf.st_ctim;
  return Optional<timespec>(ts);
}

Optional<std::string> BaseName(const std::string& path) {
  if (path.empty())
    return Optional<std::string>::Invalid();

  std::string::size_type start = path.rfind("/");
  if (start == std::string::npos) {
    return Optional<std::string>(path);
  } else {
    return Optional<std::string>(path.substr(start + 1));
  }
}

Optional<std::string> DirName(const std::string& path) {
  if (path.empty())
    return Optional<std::string>::Invalid();

  std::string::size_type start = path.rfind("/");
  if (start == std::string::npos) {
    return Optional<std::string>::Invalid();
  } else {
    return Optional<std::string>(path.substr(0, start));
  }
}

bool IsDir(const std::string& path) {
  if (path.empty())
    return false;

  struct stat stbuf;
  if (stat(path.c_str(), &stbuf) != 0)
    return false;

  return S_ISDIR(stbuf.st_mode);
}

#define DIR_MODE (S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH)

bool MakeDir(const std::string& path) {
  if (path.empty())
    return false;

  if (access(path.c_str(), F_OK) < 0) {
    if (mkdir(path.c_str(), DIR_MODE) < 0)
      return false;
  }
  return true;
}

std::string NormalDirPath(const std::string& path) {
  if (path.empty() || path == "/")
    return path;

  auto end = path.find_last_not_of("/");
  if (end == std::string::npos)
    return "/";
  return path.substr(0, end + 1);
}

Optional<std::vector<std::string>> ScanDir(
    const std::string& path,
    int (*filter)(const struct dirent*),
    int (*compar)(const struct dirent**, const struct dirent**)) {
  if (path.empty())
    return Optional<std::vector<std::string>>::Invalid();

  struct dirent **namelist = NULL;
  int num = scandir(path.c_str(), &namelist, filter, compar);
  if (num == -1)
    return Optional<std::vector<std::string>>::Invalid();

  std::vector<std::string> res;
  for (int i = 0; i < num; ++i) {
    struct dirent *dep = namelist[i];
    res.push_back(dep->d_name);
    free(dep);
  }
  free(namelist);
  return Optional<std::vector<std::string>>(res);
}

bool Rename(const std::string& oldpath, const std::string& newpath) {
  if (oldpath.empty() || newpath.empty())
    return false;

  return rename(oldpath.c_str(), newpath.c_str()) == 0;
}

Optional<time_t> StrToTs(const std::string& str, const char* format) {
  if (str.empty() || !format || format[0] == '\0')
    return Optional<time_t>::Invalid();

  struct tm timeinfo;
  memset(&timeinfo, 0, sizeof(struct tm));
  if (strptime(str.c_str(), format, &timeinfo) == NULL)
    return Optional<time_t>::Invalid();

  return Optional<time_t>(mktime(&timeinfo));
}

bool ExecuteCommand(const std::string& command, std::string* errstr) {
  if (command.empty()) {
    if (errstr)
      *errstr = "command empty";
    return false;
  }

  char buf[1024];
  bool res = true;
  int status = system(command.c_str());
  if (status < 0) {
    snprintf(buf, sizeof(buf), "system[%s] failed with errno[%d]",
             command.c_str(), errno);
    res = false;
  } else {
    if (WIFEXITED(status)) {
      if ((status = WEXITSTATUS(status)) != 0) {
        snprintf(buf, sizeof(buf), "cmd[%s] exited with errcode[%d]",
                 command.c_str(), status);
        res = false;
      }
    } else if (WIFSIGNALED(status)) {
      snprintf(buf, sizeof(buf), "cmd[%s] exited with signal[%d]",
               command.c_str(), status);
      res = false;
    } else if (WIFSTOPPED(status)) {
      snprintf(buf, sizeof(buf), "cmd[%s] stopped with signal[%d]",
               command.c_str(), WSTOPSIG(status));
      res = false;
    } else {
      snprintf(buf, sizeof(buf), "cmd[%s] exited abnormally",
               command.c_str());
      res = false;
    }
  }

  if (!res && errstr)
    *errstr = buf;
  return res;
}

}   // namespace log2hdfs
