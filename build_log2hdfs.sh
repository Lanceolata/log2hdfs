#!/usr/bin/env bash

g++ -g -W -Wall -std=c++11 \
-I src \
-I thirdparty/installed/include \
-L thirdparty/installed/lib \
-o log2kafka src/log2kafka/*.cc src/util/*.cc src/kafka/*.cc \
thirdparty/installed/include/easylogging++.cc \
-l pthread -l rdkafka -DELPP_THREAD_SAFE -DELPP_NO_DEFAULT_LOG_FILE

export LD_LIBRARY_PATH="/data/users/data-infra/log2hdfs/thirdparty/installed/lib:/usr/java/jdk1.8.0_77/jre/lib/amd64/server:/usr/hdp/2.4.0.0-169/usr/lib:/usr/hdp/2.4.0.0-169/hadoop/lib/native:$LD_LIBRARY_PATH"

./log2kafka -c conf/log2kafka_conf_example.conf -l conf/log2kafka_logconfig_example.conf
