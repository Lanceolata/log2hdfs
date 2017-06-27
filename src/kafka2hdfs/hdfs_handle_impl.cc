// Copyright (c) 2017 Lanceolata

#include "kafka2hdfs/hdfs_handle_impl.h"
#include "util/configparser.h"
#include "util/system_utils.h"
#include "easylogging++.h"

namespace log2hdfs {

// ------------------------------------------------------------------
// HdfsHandle

std::shared_ptr<HdfsHandle> HdfsHandle::Init(
    std::shared_ptr<Section> section) {
  if (!section) {
    LOG(ERROR) << "HdfsHandle Init invalid parameters";
    return nullptr;
  }

  std::shared_ptr<HdfsHandle> res;
  std::string type = section->Get("type", "");
  if (type == "command") {
    res = CommandHdfsHandle::Init(std::move(section));
  } else {
    LOG(ERROR) << "HdfsHandle Init invalid type[" << type << "]";
  }
  return res;
}

// ------------------------------------------------------------------
// CommandHdfsHandle

#define DEFAULT_HDFS_PUT "hadoop fs -put"
#define DEFAULT_HDFS_APPEND "hadoop fs -appendToFile"
#define DEFAULT_HDFS_LZO_INDEX "hadoop jar /usr/hdp/2.4.0.0-169/hadoop/lib/" \
    "hadoop-lzo-0.6.0.2.4.0.0-169.jar com.hadoop.compression.lzo.LzoIndexer"

std::shared_ptr<CommandHdfsHandle> CommandHdfsHandle::Init(
    std::shared_ptr<Section> section) {
  if (!section) {
    LOG(ERROR) << "CommandHdfsHandle Init invalid parameters";
    return nullptr;
  }

  std::string put = section->Get("put", DEFAULT_HDFS_PUT);
  std::string append = section->Get("append", DEFAULT_HDFS_APPEND);
  std::string lzo_index = section->Get("lzo.index", DEFAULT_HDFS_LZO_INDEX);

  if (put.empty() || append.empty() || lzo_index.empty()) {
    LOG(ERROR) << "CommandHdfsHandle Init invalid parameters"
               << "put[" << put << "] "
               << "append[" << append << "] "
               << "lzo.index[" << lzo_index << "]";
    return nullptr;
  }

  std::string namenode = section->Get("namenode", "");
  std::string port_str = section->Get("port", "");
  std::string user = section->Get("user", "");
  tPort port = static_cast<tPort>(atoi(port_str.c_str()));
  if (namenode.empty() || user.empty() || port <= 0) {
    LOG(ERROR) << "CommandHdfsHandle Init invalid parameters namenode["
               << namenode << "] port[" << port << "] user["
               << user << "]";
    return nullptr;
  }

  hdfsFS fs_handle = hdfsConnectAsUser(namenode.c_str(), port, user.c_str());
  if (!fs_handle) {
    LOG(ERROR) << "CommandHdfsHandle Init hdfsConnectAsUser failed namenode["
               << namenode << "] port[" << port << "] user["
               << user << "]";
    return nullptr;
  }

  LOG(INFO) << "CommandHdfsHandle Init success namenode[" << namenode
            << "] port[" << port << "] user[" << user << "] put[" << put
            << "] append[" << append << "] index[" << lzo_index << "]";
  return std::make_shared<CommandHdfsHandle>(fs_handle,
             put, append, lzo_index);
}

bool CommandHdfsHandle::Exists(const std::string& hdfs_path) const {
  if (hdfs_path.empty())
    return false;

  return hdfsExists(fs_handle_, hdfs_path.c_str()) == 0;
}

bool CommandHdfsHandle::Put(const std::string& local_path,
                            const std::string& hdfs_path) const {
  if (local_path.empty() || hdfs_path.empty())
    return false;

  std::string cmd = put_ + " " + local_path + " " + hdfs_path;
  std::string errstr;
  if (ExecuteCommand(cmd, &errstr)) {
    return true;
  } else {
    LOG(WARNING) << "CommandHdfsHandle Put ExecuteCommand[" << cmd
                 << "] failed with errstr[" << errstr << "]";
    return false;
  }
}

bool CommandHdfsHandle::Append(const std::string& local_path,
                               const std::string& hdfs_path) const {
  if (local_path.empty() || hdfs_path.empty())
    return false;

  std::string cmd = append_ + " " + local_path + " " + hdfs_path;
  std::string errstr;
  if (ExecuteCommand(cmd, &errstr)) {
    return true;
  } else {
    LOG(WARNING) << "CommandHdfsHandle Put ExecuteCommand[" << cmd
                 << "] failed with errstr[" << errstr << "]";
    return false;
  }
}

bool CommandHdfsHandle::Delete(const std::string& hdfs_path) const {
  if (hdfs_path.empty())
    return false;

  return hdfsDelete(fs_handle_, hdfs_path.c_str(), 0) == 0;
}

bool CommandHdfsHandle::CreateDirectory(const std::string& hdfs_path) const {
  if (hdfs_path.empty())
    return false;

  return hdfsCreateDirectory(fs_handle_, hdfs_path.c_str()) == 0;
}

bool CommandHdfsHandle::LZOIndex(const std::string& hdfs_path) const {
  if (hdfs_path.empty())
    return false;

  std::string cmd = lzo_index_ + " " + hdfs_path;
  std::string errstr;
  if (ExecuteCommand(cmd, &errstr)) {
    return true;
  } else {
    LOG(WARNING) << "CommandHdfsHandle LZOIndex ExecuteCommand[" << cmd
                 << "] failed with errstr[" << errstr << "]";
    return false;
  }
}

}   // namespace log2hdfs
