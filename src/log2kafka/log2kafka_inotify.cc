// Copyright (c) 2017 Lanceolata

#include "log2kafka/log2kafka_inotify.h"
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <sys/inotify.h>
#include "log2kafka/log2kafka_offset_table.h"
#include "log2kafka/log2kafka_topic_conf.h"
#include "util/system_utils.h"
#include "util/string_utils.h"
#include "easylogging++.h"

namespace log2hdfs {

namespace {

int set_nonblocking(int fd) {
  int old = fcntl(fd, F_GETFL);
  if (old == -1) {
    LOG(ERROR) << "fcntl F_GETFL failed with errno:" << errno;
    return -1;
  }

  if (fcntl(fd, F_SETFL, old | O_NONBLOCK) == -1) {
    LOG(ERROR) << "fcntl F_SETFL failed with errno:" << errno;
    return -1;
  }
  return 0;
}

int inotify_fd() {
  int inot_fd = inotify_init();
  if (inot_fd == -1) {
    LOG(ERROR) <<  "inotify_init failed with errno:" << errno;
    return -1;
  }

  if (set_nonblocking(inot_fd) != 0) {
    close(inot_fd);
    return -1;
  }
  return inot_fd;
}

inline int AddWatch(int fd, const std::string& path) {
  return inotify_add_watch(fd, path.c_str(),
            IN_MOVED_TO | IN_CREATE | IN_DONT_FOLLOW | IN_DELETE_SELF);
}

inline bool RemoveWatch(int fd, int wd) {
  if (inotify_rm_watch(fd, wd) == -1)
    return false;
  return true;
}

int scandir_filter(const struct dirent *dep) {
  if (strncmp(dep->d_name, ".", 1) == 0)
    return 0;
  return 1;
}

int scandir_compar(const struct dirent **a, const struct dirent **b) {
  return strcmp((*a)->d_name, (*b)->d_name);
}

}   // namespace

std::unique_ptr<Log2kafkaInotify> Log2kafkaInotify::Init(
    std::shared_ptr<Queue<std::string>> queue,
    std::shared_ptr<Log2kafkaOffsetTable> table) {
  if (!queue || !table)
    return nullptr;

  int inot_fd = inotify_fd();
  if (inot_fd == -1)
    return nullptr;

  return std::unique_ptr<Log2kafkaInotify>(new Log2kafkaInotify(
             inot_fd, std::move(queue), std::move(table)));
}

bool Log2kafkaInotify::AddWatchTopic(
    std::shared_ptr<Log2kafkaTopicConf> conf) {
  for (auto& path : conf->dir_path()) {
    if (!AddWatchPath(conf->topic_name(), path, conf->remedy_expire())) {
      LOG(ERROR) << "Log2kafkaInotify AddWatchTopic AddWatchPath topic["
                   << conf->topic_name() << "] path[" << path
                   << "] remedy[" << conf->remedy_expire() << "] failed";
      return false;
    }
  }
  return true;
}

bool Log2kafkaInotify::RemoveWatchTopic(const std::string& topic) {
  if (topic.empty()) {
    LOG(WARNING) << "Log2kafkaInotify RemoveWatchTopic failed empty topic";
    return false;
  }

  std::lock_guard<std::mutex> guard(mutex_);
  for (auto it = wd_topic_.begin(); it != wd_topic_.end();) {
    if (it->second != topic) {
      ++it;
      continue;
    }

    int wd = it->first;
    if (!RemoveWatch(inot_fd_, wd))
      LOG(WARNING) << "Log2kafkaInotify RemoveWatchTopic RemoveWatch wd["
                   << wd << "] path[" << wd_path_[wd] << "] failed with "
                   << "errno[" << errno << "]";

    LOG(INFO) << "RemoveWatchTopic RemoveWatch topic[" << topic
              << "] path[" << wd_path_[wd] << "]";
    wd_topic_.erase(it++);
    wd_path_.erase(wd);
  }
  return true;
}

bool Log2kafkaInotify::AddWatchPath(const std::string& topic,
    const std::string& path, time_t remedy) {
  if (topic.empty() || path.empty() || remedy < -1) {
    LOG(WARNING) << "Log2kafkaInotify AddWatchPath topic[" << topic
                 << "] path[" << path << "] remedy[" << remedy
                 << "] failed";
    return false;
  }

  std::string normalpath = NormalDirPath(path);
  if (!IsDir(normalpath)) {
    LOG(ERROR) << "Log2kafkaInotify AddWatchPath topic[" << topic
               << "] normalpath[" << normalpath << "] not dir";
    return false;
  }

  while (true) {
    Optional<time_t> mtime = FileMtime(normalpath);
    if (!mtime.valid()) {
      LOG(ERROR) << "Log2kafkaInotify AddWatchPath topic[" << topic
                 << "] FileMtime[" << normalpath << "] failed";
      return false;
    }

    if (time(NULL) > mtime.value() + 3) {
      break;
    }
    sleep(1);
  }

  std::unique_lock<std::mutex> guard(mutex_);
  int wd = AddWatch(inot_fd_, normalpath);
  if (wd == -1) {
    LOG(ERROR) << "Log2kafkaInotify AddWatchPath AddWatch topic[" << topic
               << "] normalpath[" << normalpath << "] failed with errno["
               << errno << "]";
    return false;
  }

  struct timespec ts;
  memset(&ts, 0, sizeof(ts));
  if (clock_gettime(CLOCK_REALTIME, &ts) == -1) {
    LOG(ERROR) << "Log2kafkaInotify AddWatchPath clock_gettime topic["
               << topic << "] normalpath[" << normalpath
               << "] failed with errno[" << errno << "]";
    return false;
  }

  auto it = wd_topic_.find(wd);
  if (it != wd_topic_.end()) {
    LOG(WARNING) << "Log2kafkaInotify AddWatchPath topic[" << topic
                 << "] normalpath[" << normalpath << "] already watched";
    return false;
  }

  wd_topic_[wd] = topic;
  wd_path_[wd] = normalpath;
  guard.unlock();

  LOG(INFO) << "Log2kafkaInotify AddWatchPath topic[" << topic
            << "] normalpath[" << normalpath << "] remedy[" << remedy
            << "] successed";

  Remedy(topic, normalpath, remedy, ts);
  return true;
}

void Log2kafkaInotify::Remedy(const std::string& topic,
    const std::string& dir, time_t remedy, const struct timespec& ts) {
  std::string remedy_file;
  off_t remedy_offset;
  Optional<struct timespec> remedy_ctime;
  if (table_->Get(dir, &remedy_file, &remedy_offset)) {
    std::string remedy_path = dir + "/" + remedy_file;
    remedy_ctime = FileCtime(remedy_path);
    if (!remedy_ctime.valid()) {
      LOG(WARNING) << "Log2kafkaInotify Remedy topic[" << topic
                   << "] FileCtime[" << remedy_path << "] failed";
    }
    LOG(INFO) << "Log2kafkaInotify Remedy push topic[" << topic
              << "] path[" << remedy_path << "] offset["
              << remedy_offset << "]";
    queue_->Push(topic + ":" + remedy_path + ":" +
                 std::to_string(remedy_offset));
  }

  Optional<std::vector<std::string>> names =
      ScanDir(dir, scandir_filter, scandir_compar);
  if (!names.valid()) {
    LOG(WARNING) << "Log2kafkaInotify Remedy ScanDir[" << dir << "] failed";
    return;
  }

  for (auto& name : names.value()) {
    std::string inner = dir + "/" + name;
    if (IsDir(inner)) {
      AddWatchPath(topic, inner, remedy);
    } else if (IsFile(inner)) {
      if (remedy == -1)
        continue;

      Optional<timespec> ctime = FileCtime(inner);
      if (!ctime.valid()) {
        LOG(WARNING) << "Log2kafkaInotify Remedy FileCtime:" << inner
                     << " failed";
        continue;
      }

      if (remedy != 0) {
        if (ctime.value().tv_sec < time(NULL) - remedy)
          continue;
      }

      // ctime less than ts
      if (ctime.value().tv_sec > ts.tv_sec) {
        continue;
      } else if (ctime.value().tv_sec == ts.tv_sec) {
        if (ctime.value().tv_nsec >= ts.tv_nsec)
          continue;
      }

      // ctime more than remedy ctime
      if (remedy_ctime.valid()) {
        if (ctime.value().tv_sec < remedy_ctime.value().tv_sec) {
          continue;
        } else if (ctime.value().tv_sec == remedy_ctime.value().tv_sec) {
          if (ctime.value().tv_nsec <= remedy_ctime.value().tv_nsec)
            continue;
        }
      }

      LOG(INFO) << "Log2kafkaInotify Remedy push topic[" << topic
                << "] path[" << inner << "]";
      queue_->Push(topic + ":" + inner + ":0");
    } else {
      LOG(WARNING) << "Log2kafkaInotify Remedy Unknown file[" << inner << "]";
    }
  }
}

#define INOT_BUF_LEN 8192

void Log2kafkaInotify::StartInternal() {
  LOG(INFO) << "Log2kafkaInotify thread created";

  struct pollfd pfd = { inot_fd_, POLLIN | POLLPRI, 0 };
  int n;

  while (true) {
    n = poll(&pfd, 1, 120000);
    if (n == -1) {
      if (errno == EINTR) {
        LOG(WARNING) << "Log2kafkaInotify poll interrupted by a signal";
        continue;
      }
      LOG(ERROR) << "Log2kafkaInotify poll failed with errno:" << errno;
      LOG(ERROR) << "Log2kafkaInotify thread exiting";
      return;
    } else if (n == 0) {
      LOG(INFO) << "Log2kafkaInotify poll timed out after 120 seconds";
      continue;
    }

    if ((pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
      LOG(WARNING) << "Log2kafkaInotify poll abnormal";
      continue;
    }
    if (ReadInotify() < 0)
      break;
  }

  LOG(INFO) << "Log2kafkaInotify thread existing";
}

int Log2kafkaInotify::ReadInotify() {
  char buf[INOT_BUF_LEN]
      __attribute__((aligned(__alignof__(struct inotify_event))));

  struct inotify_event *evp;
  int num = 0;

  while (true) {
    ssize_t n = read(inot_fd_, buf, sizeof(buf));
    if (n == -1) {
      if (errno == EAGAIN) {
        LOG(INFO) << "Log2kafkaInotify ReadInotify reading inotify "
                  << "FD would block";
      } else {
        LOG(ERROR) << "reading inotify FD failed with errno:" << errno;
        num = -1;
      }
      break;
    } else if (n == 0) {
      LOG(INFO) << "no inotify events read";
      break;
    } else if (n == INOT_BUF_LEN) {
      LOG(WARNING) << "inotify buf might overflow";
    }

    std::string topic, path;
    for (char *p = buf; p < buf + n;
        p += sizeof(struct inotify_event) + evp->len) {
      evp = (struct inotify_event *)p;
      int wd = evp->wd;

      std::unique_lock<std::mutex> guard(mutex_);
      auto it1 = wd_topic_.find(wd);
      if (it1 == wd_topic_.end()) {
        LOG(WARNING) << "Log2kafkaInotify ReadInotify Unknown wd:" << wd;
        continue;
      }
      topic = it1->second;

      auto it2 = wd_path_.find(wd);
      if (it2 == wd_path_.end()) {
        LOG(WARNING) << "Log2kafkaInotify ReadInotify Unknown wd path:" << wd;
        continue;
      }
      path = it2->second;
      guard.unlock();

      if ((evp->mask & IN_ISDIR) != 0) {
        if ((evp->mask & IN_CREATE) != 0) {
          path.append("/");
          path.append(evp->name);
          AddWatchPath(topic, path, 0);
        } else {
          LOG(WARNING) << "unexpected inotify dir event" << evp->mask;
        }
      } else if ((evp->mask & IN_MOVED_TO) != 0) {
        path.append("/");
        path.append(evp->name);
        queue_->Push(topic + ":" + path + ":0");
      } else if ((evp->mask & IN_DELETE_SELF) != 0) {
        guard.lock();
        if (!RemoveWatch(inot_fd_, wd)) {
          LOG(WARNING) << "RemoveWatch wd[" << wd << "] path[" << path
                       << "] faield with errno[" << errno << "]";
        }

        LOG(INFO) << "ReadInotify RemoveWatch topic:" << topic
                  << " path:" << path;
        wd_topic_.erase(wd);
        wd_path_.erase(wd);
        guard.unlock();

        table_->Remove(path);
      }
    }
  }
  return num;
}

}   // namespace log2hdfs
