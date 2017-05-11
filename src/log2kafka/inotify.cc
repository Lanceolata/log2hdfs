// Copyright (c) 2017 Lanceolata

#include "log2kafka/inotify.h"
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <sys/inotify.h>
#include "log2kafka/offset_table.h"
#include "log2kafka/topic_conf.h"
#include "util/system_utils.h"
#include "util/string_utils.h"
#include "easylogging++.h"

namespace log2hdfs {

namespace {

int set_nonblocking(int fd) {
  int old = fcntl(fd, F_GETFL);
  if (old == -1) {
    LOG(ERROR) << "set_nonblocking fcntl F_GETFL failed with errno["
               << errno << "]";
    return -1;
  }

  if (fcntl(fd, F_SETFL, old | O_NONBLOCK) == -1) {
    LOG(ERROR) << "set_nonblocking fcntl F_SETFL failed with errno["
               << errno << "]";
    return -1;
  }
  return 0;
}

int inotify_fd() {
  int inot_fd = inotify_init();
  if (inot_fd == -1) {
    LOG(ERROR) <<  "inotify_fd inotify_init failed with errno["
               << errno << "]";
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
             IN_MOVED_TO | IN_CREATE | IN_DONT_FOLLOW);
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

std::unique_ptr<Inotify> Inotify::Init(
    std::shared_ptr<Queue<std::string>> queue,
    std::shared_ptr<OffsetTable> table) {
  if (!queue || !table) {
    LOG(ERROR) << "Inotify Init invalid parameters";
    return nullptr;
  }

  int inot_fd = inotify_fd();
  if (inot_fd == -1) {
    LOG(ERROR) << "Inotify Init inotify_fd failed";
    return nullptr;
  }

  return std::unique_ptr<Inotify>(new Inotify(
             inot_fd, std::move(queue), std::move(table)));
}

bool Inotify::AddWatchTopic(std::shared_ptr<TopicConf> conf) {
  const std::string& topic = conf->topic();
  LOG(INFO) << "Inotify AddWatchTopic topic[" << topic << "]";
  for (auto& path : conf->dirs()) {
    if (!AddWatchPath(topic, path, conf->remedy())) {
      LOG(WARNING) << "Inotify AddWatchTopic AddWatchPath topic["
                   << topic << "] path[" << path << "] remedy["
                   << conf->remedy() << "] failed";
      return false;
    }
  }
  return true;
}

bool Inotify::RemoveWatchTopic(const std::string& topic) {
  if (topic.empty()) {
    LOG(WARNING) << "Inotify RemoveWatchTopic invalid parameters";
    return false;
  }

  std::lock_guard<std::mutex> guard(mutex_);
  for (auto it = wd_topic_.begin(); it != wd_topic_.end(); ++it) {
    if (it->second != topic)
      continue;

    std::string path = wd_path_[it->first];
    if (RemoveWatch(inot_fd_, it->first)) {
      LOG(INFO) << "Inotify RemoveWatchTopic RemoveWatch wd[" << it->first
                << "] topic[" << topic << "] path[" << path << "] success";
    } else {
      LOG(WARNING) << "Inotify RemoveWatchTopic RemoveWatch wd[" << it->first
                   << "] topic[" << topic << "] path[" << path
                   << "] failed with errno[" << errno << "]";
    }
  }
  return true;
}

bool Inotify::AddWatchPath(const std::string& topic,
                           const std::string& path,
                           time_t remedy) {
  if (topic.empty() || path.empty() || remedy < -1) {
    LOG(WARNING) << "Inotify AddWatchPath invalid parameters topic["
                 << topic << "] path[" << path << "] remedy[" << remedy
                 << "]";
    return false;
  }

  std::string normalpath = NormalDirPath(path);
  if (!IsDir(normalpath)) {
    LOG(WARNING) << "Inotify AddWatchPath topic[" << topic << "] path["
                 << normalpath << "] not dir";
    return false;
  }

  // wait for dir not change.
  while (true) {
    time_t mtime = FileMtime(normalpath);
    if (mtime < 0) {
      LOG(ERROR) << "Inotify AddWatchPath topic[" << topic
                 << "] FileMtime[" << normalpath << "] failed";
      return false;
    }

    if (time(NULL) > mtime + 3) {
      break;
    }
    sleep(1);
  }

  // step 1: Get remedy file and path
  std::string remedy_file = "";
  off_t remedy_offset = -1;
  table_->Get(normalpath, &remedy_file, &remedy_offset);

  // step 2: Add watch
  std::unique_lock<std::mutex> guard(mutex_);
  int wd = AddWatch(inot_fd_, normalpath);
  if (wd == -1) {
    LOG(ERROR) << "Inotify AddWatchPath AddWatch topic[" << topic
               << "] path[" << normalpath << "] failed with errno["
               << errno << "]";
    return false;
  }

  // step 3: Get add watch timespec
  struct timespec ts;
  memset(&ts, 0, sizeof(ts));
  if (clock_gettime(CLOCK_REALTIME, &ts) == -1) {
    LOG(ERROR) << "Inotify AddWatchPath clock_gettime topic[" << topic
               << "] path[" << normalpath << "] failed with errno["
               << errno << "]";
    return false;
  }

  // step 4: Check wd_topic_ avoid repeat remedy.
  auto it = wd_topic_.find(wd);
  if (it != wd_topic_.end()) {
    LOG(WARNING) << "Inotify AddWatchPath topic[" << topic << "] path["
                 << normalpath << "] already watched";
    return false;
  }

  // step 5: Update wd_topic_ and wd_path_
  wd_topic_[wd] = topic;
  wd_path_[wd] = normalpath;
  guard.unlock();

  LOG(INFO) << "Inotify AddWatchPath topic[" << topic << "] path["
            << normalpath << "] remedy[" << remedy << "] successed";

  // step 6: Remedy records already in dir
  Remedy(topic, normalpath, remedy, ts, remedy_file, remedy_offset);
  return true;
}

void Inotify::Remedy(const std::string& topic, const std::string& dir,
                     time_t remedy, const struct timespec& end,
                     const std::string& remedy_file, off_t remedy_offset) {
  Optional<struct timespec> start;
  if (!remedy_file.empty() && remedy_offset >= 0) {
    struct timespec start_ts;
    std::string remedy_path = dir + "/" + remedy_file;
    if (FileCtim(remedy_path, &start_ts)) {
      start = Optional<struct timespec>(start_ts);
    } else {
      LOG(WARNING) << "Inotify Remedy topic[" << topic << "] FileCtim "
                   << "remedy path[" << remedy_path << "] failed with errno["
                   << errno << "]";
    }

    LOG(INFO) << "Inotify Remedy push topic[" << topic << "] path["
              << remedy_path << "] offset[" << remedy_offset << "]";
    queue_->Push(topic + ":" + remedy_path + ":" +
                 std::to_string(remedy_offset));
  }

  std::vector<std::string> names;
  if (!ScanDir(dir, scandir_filter, scandir_compar, &names)) {
    LOG(WARNING) << "Inotify Remedy topic[" << topic << "] ScanDir["
                 << dir << "failed with errno[" << errno << "]";
    return;
  }

  for (auto& name : names) {
    std::string inner = dir + "/" + name;
    if (IsDir(inner)) {
      // sub dictionary
      AddWatchPath(topic, inner, remedy);
    } else if (IsFile(inner)) {
      // sub file
      if (remedy == -1)
        continue;

      struct timespec ctime;
      if (!FileCtim(inner, &ctime)) {
        LOG(WARNING) << "Inotify Remedy topic[" << topic << "] FileCtim["
                     << inner << "failed with errno[" << errno << "]";
        continue;
      }

      if (remedy != 0) {
        if (ctime.tv_sec < time(NULL) - remedy)
          continue;
      }

      // ctime less than end
      if (ctime.tv_sec > end.tv_sec) {
        continue;
      } else if (ctime.tv_sec == end.tv_sec) {
        if (ctime.tv_nsec >= end.tv_nsec)
          continue;
      }

      // ctime more than start
      if (start.valid()) {
        if (ctime.tv_sec < start.value().tv_sec) {
          continue;
        } else if (ctime.tv_sec == start.value().tv_sec) {
          if (ctime.tv_nsec <= start.value().tv_nsec)
            continue;
        }
      }

      LOG(INFO) << "Inotify Remedy push topic[" << topic << "] path["
                << inner << "]";
      queue_->Push(topic + ":" + inner + ":0");
    } else {
      LOG(WARNING) << "Inotify Remedy unknown file[" << inner << "]";
    }
  }
}

void Inotify::StartInternal() {
  LOG(INFO) << "Inotify thread created";

  struct pollfd pfd = { inot_fd_, POLLIN | POLLPRI, 0 };
  int n;

  while (true) {
    n = poll(&pfd, 1, 120000);
    if (n == -1) {
      if (errno == EINTR) {
        LOG(WARNING) << "Inotify StartInternal poll interrupted by a signal";
        continue;
      }
      LOG(ERROR) << "Inotify StartInternal poll failed with errno["
                 << errno << "]";
      break;
    } else if (n == 0) {
      LOG(INFO) << "Inotify StartInternal poll timed out after 120 seconds";
      continue;
    }

    if ((pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
      LOG(WARNING) << "Inotify StartInternal poll abnormal";
      continue;
    }

    if (ReadInotify() < 0)
      break;
  }

  LOG(ERROR) << "Inotify thread existing";
}

#define INOT_BUF_LEN 8192

int Inotify::ReadInotify() {
  char buf[INOT_BUF_LEN]
      __attribute__((aligned(__alignof__(struct inotify_event))));

  struct inotify_event *evp;
  int num = 0;

  std::unique_lock<std::mutex> guard(mutex_, std::defer_lock);
  while (true) {
    ssize_t n = read(inot_fd_, buf, sizeof(buf));
    if (n == -1) {
      if (errno != EAGAIN) {
        LOG(ERROR) << "Inotify ReadInotify fd failed with errno["
                   << errno << "]";
        num = -1;
      }
      break;
    } else if (n == 0) {
      LOG(INFO) << "Inotify ReadInotify no inotify events read";
      break;
    } else if (n == INOT_BUF_LEN) {
      LOG(WARNING) << "Inotify ReadInotify buf might overflow";
    }

    std::string topic, path;
    for (char *p = buf; p < buf + n;
            p += sizeof(struct inotify_event) + evp->len) {
      ++num;
      evp = (struct inotify_event *)p;
      int wd = evp->wd;

      guard.lock();
      auto it1 = wd_topic_.find(wd);
      if (it1 == wd_topic_.end()) {
        LOG(WARNING) << "Inotify ReadInotify unknown wd topic[" << wd << "]";
        guard.unlock();
        continue;
      }
      topic = it1->second;

      auto it2 = wd_path_.find(wd);
      if (it2 == wd_path_.end()) {
        LOG(WARNING) << "Inotify ReadInotify unknown wd path[" << wd
                     << "] topic[" << topic << "]";
        guard.unlock();
        continue;
      }
      path = it2->second;
      guard.unlock();

      if ((evp->mask & IN_ISDIR) != 0) {
        // new dir create
        if ((evp->mask & IN_CREATE) != 0) {
          path.append("/");
          path.append(evp->name);
          AddWatchPath(topic, path, 0);
        } else {
          LOG(WARNING) << "Inotify ReadInotify wd[" << wd << "] topic["
                       << topic << "] path[" << path << "] unknown mask["
                       << evp->mask << "]";
        }
      } else if ((evp->mask & IN_MOVED_TO) != 0) {
        // new file move to
        path.append("/");
        path.append(evp->name);
        queue_->Push(topic + ":" + path + ":0");
      } else if ((evp->mask & IN_IGNORED) != 0) {
        // Watch was removed explicitly (inotify_rm_watch(2)) or
        // automatically(file was deleted, or filesystem was unmounted).
        LOG(INFO) << "Inotify ReadInotify erase wd[" << wd << "] topic["
                  << topic << "] path[" << path << "]";

        guard.lock();
        wd_topic_.erase(wd);
        wd_path_.erase(wd);
        guard.unlock();

        table_->Remove(path);
      } else {
        LOG(WARNING) << "Inotify ReadInotify wd[" << wd << "] topic[" << topic
                     << "] path[" << path << "] unknown mask[" << evp->mask
                     << "]";
      }
    }
  }
  return num;
}

}   // namespace log2hdfs
