#include <iostream>
#include "gtest/gtest.h"
#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(int argc, char *argv[]) {
  int opt = 0;
  char *conf_path = NULL, *log_conf_path = NULL;
  while (opt != -1) {
    opt = getopt(argc, argv, "c:l:");
    switch (opt) {
      case 'c':
        conf_path = optarg;
        std::cout << "conf path for kafka2hdfs:" << conf_path;
        break;
      case 'l':
        log_conf_path = optarg;
        std::cout << "log cong path for kafka2hdfs" << log_conf_path;
        break;
      default:
        std::cout << "unknown opt:" << opt;
        continue;
    }
  }
/*
  if (log_conf_path) {
    el::Configurations log_conf(log_conf_path);
    el::Loggers::reconfigureAllLoggers(log_conf);
  }
*/  
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
