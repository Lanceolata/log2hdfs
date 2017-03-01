// Copyright (c) 2017 Lanceolata

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <iostream>
#include "kafka2hdfs/hdfs_handle.h"
#include "kafka/kafka_conf.h"
#include "util/logger.h"
#include "util/configparser.h"

using namespace log2hdfs;

#define DEFAULT_LOG_SIZE "2048"

int main(int argc, char *argv[]) {
  int opt;
  char *conf_path = NULL;

  opt = getopt(argc, argv, "c:");
  while (opt != -1) {
    switch (opt) {
      case 'c':
        conf_path = optarg;
        std::cerr << "Config path for kafka2hdfs: " << conf_path << std::endl;
        break;
      default:
        std::cerr << "Usage: ./kafka2hdfs -c conf_path" << std::endl;
    }
    opt = getopt(argc, argv, "c:");
  }

  if (conf_path == NULL) {
    std::cerr << "Usage: ./kafka2hdfs -c conf_path" << std::endl;
    exit(EXIT_FAILURE);
  }

  // Init Conf
  std::shared_ptr<IniConfigParser> conf = IniConfigParser::Init("=");
  if (!conf) {
    std::cerr << "IniConfigParser init failed" << std::endl;
    exit(EXIT_FAILURE);
  }

  if (!conf->Read(conf_path)) {
    std::cerr << "Read config path failed" << std::endl;
    exit(EXIT_FAILURE);
  }

  for (auto it1 = conf->Begin(); it1 != conf->End(); ++it1) {
    std::cerr << "[" << it1->first << "]" << std::endl;
    for (auto it2 = it1->second->Begin(); it2 != it1->second->End(); ++it2) {
        std::cerr << it2->first << " = " << it2->second << std::endl;
    }
    std::cerr << std::endl;
  }

  // Init Log
  Optional<std::string> opt_log_path = conf->Get("global", "log.path");
  if (!opt_log_path.valid()) {
    std::cerr << "Get log.path failed" << std::endl;
    exit(EXIT_FAILURE);
  }

  std::string str_log_size = conf->Get("global", "log.size", DEFAULT_LOG_SIZE);
  size_t log_size = static_cast<unsigned int>(atoi(str_log_size.c_str()));
  std::shared_ptr<Logger> logger = Logger::Init(opt_log_path.value(), log_size);
  if (!LogInit(logger)) {
    std::cerr << "LogInit failed with path[" << opt_log_path.value()
        << "] size[" << log_size << "]" << std::endl;
    exit(EXIT_FAILURE);
  }

  // Init HDFS handle
  Log(LogLevel::kLogInfo, "Init hdfs handle");
  Optional<std::string> opt_namenode = conf->Get("hdfs", "namenode");
  Optional<std::string> opt_port = conf->Get("hdfs", "port");
  Optional<std::string> opt_user = conf->Get("hdfs", "user");

  if (!opt_namenode.valid() || !opt_port.valid() || !opt_user.valid()) {
    Log(LogLevel::kLogError, "Get hdfs info failed namenode[%s] port[%s] "
        "user[%s]", opt_namenode.value().c_str(), opt_port.value().c_str(),
        opt_user.value().c_str());
    exit(EXIT_FAILURE);
  }

  uint16_t port = static_cast<uint16_t>(atoi(opt_port.value().c_str()));
  std::shared_ptr<HdfsHandle> hdfs_handle = HdfsHandle::Init(
      opt_namenode.value(), port, opt_user.value().c_str());

  if (!hdfs_handle) {
    Log(LogLevel::kLogError, "Init HdfsHandle failed namenode[%s] port[%s] "
        "user[%s]", opt_namenode.value().c_str(), opt_port.value().c_str(),
        opt_user.value().c_str());
    exit(EXIT_FAILURE);
  }

  std::cout << "/user/data-infra/express_bid" << std::endl;
  std::cout << hdfs_handle->FileExists("/user/data-infra/express_bid") << std::endl;

  std::cout << "/user/data-infra/log2hdfs_test/unbid" << std::endl;
  std::cout <<  hdfs_handle->CreateDirectory("/user/data-infra/log2hdfs_test/unbid") << std::endl;

  // Init kafka consumer
  
  // Init kafka consumer conf
  std::unique_ptr<GlobalConf> consumer_conf = GlobalConf::Init(
      GlobalConf::ConfType::kConfConsumer);
  if (!consumer_conf) {
    Log(LogLevel::kLogError, "Init consumer global conf failed");
    exit(EXIT_FAILURE);
  }

  Log(LogLevel::kLogInfo, "Init kafka consumer");
  return 0;
}
