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

off_t FileSize(const std::string& path) {
  if (path.empty())
    return -1;

  struct stat stbuf;
  if (stat(path.c_str(), &stbuf) != 0)
    return -1;

  return stbuf.st_size;
}

time_t FileAtime(const std::string& path) {
  if (path.empty())
    return -1;

  struct stat stbuf;
  if (stat(path.c_str(), &stbuf) != 0)
    return -1;

  return stbuf.st_atime;
}

time_t FileMtime(const std::string& path) {
  if (path.empty())
    return -1;

  struct stat stbuf;
  if (stat(path.c_str(), &stbuf) != 0)
    return -1;

  return stbuf.st_mtime;
}

bool FileCtim(const std::string& path, struct timespec* ts) {
  if (path.empty() || !ts)
    return false;

  struct stat stbuf;
  if (stat(path.c_str(), &stbuf) != 0)
    return false;

  *ts = stbuf.st_ctim;
  return true;
}

std::string BaseName(const std::string& path) {
  if (path.empty())
    return "";

  std::string::size_type start = path.rfind("/");
  if (start == std::string::npos) {
    return path;
  } else {
    return path.substr(start + 1);
  }
}

std::string DirName(const std::string& path) {
  if (path.empty())
    return "";

  std::string::size_type end = path.rfind("/");
  if (end == std::string::npos) {
    return "";
  } else if (end == 0) {
    return "/";
  } else {
    return path.substr(0, end);
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

  if (access(path.c_str(), F_OK) != 0) {
    if (mkdir(path.c_str(), DIR_MODE) != 0)
      return false;
  }
  return true;
}

std::string NormalDirPath(const std::string& path) {
  if (path.empty())
    return "";

  std::string::size_type end = path.find_last_not_of("/");
  if (end == std::string::npos)
    return "/";
  return path.substr(0, end + 1);
}

bool ScanDir(
    const std::string& path,
    int (*filter)(const struct dirent*),
    int (*compar)(const struct dirent**, const struct dirent**),
    std::vector<std::string>* names) {
  if (path.empty() || !names)
    return false;

  struct dirent **namelist = NULL;
  int num = scandir(path.c_str(), &namelist, filter, compar);
  if (num == -1)
    return false;

  (*names).clear();
  for (int i = 0; i < num; ++i) {
    struct dirent *dep = namelist[i];
    (*names).push_back(dep->d_name);
    free(dep);
  }
  free(namelist);
  return true;
}

time_t StrToTs(const std::string& str, const char* format) {
  if (str.empty() || !format || format[0] == '\0')
    return -1;

  struct tm timeinfo;
  memset(&timeinfo, 0, sizeof(struct tm));
  if (strptime(str.c_str(), format, &timeinfo) == NULL)
    return -1;

  return mktime(&timeinfo);
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
