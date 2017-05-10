#!/usr/bin/env bash

g++ -g -std=c++11 \
-I src \
-I thirdparty/installed/include \
-L thirdparty/installed/lib \
-o log2kafka src/log2kafka/*.cc src/util/*.cc src/kafka/*.cc \
thirdparty/installed/include/easylogging++.cc \
-l pthread -l rdkafka -DELPP_THREAD_SAFE -DELPP_NO_DEFAULT_LOG_FILE

export LD_LIBRARY_PATH="thirdparty/installed/lib:$LD_LIBRARY_PATH"

./log2kafka -c conf/log2kafka_conf_example.conf -l conf/log2kafka_logconfig_example.conf >> stderr.log 2>&1 &
