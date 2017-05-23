#!/usr/bin/env bash

g++ -g -W -Wall -std=c++11 \
-I src \
-I thirdparty/installed/include \
-I /usr/hdp/2.4.0.0-169/usr/include/ \
-L thirdparty/installed/lib \
-L /usr/java/jdk1.8.0_77/jre/lib/amd64/server \
-L /usr/hdp/2.4.0.0-169/usr/lib \
-o bin/kafka2hdfs src/kafka2hdfs/*.cc src/kafka/*.cc src/util/*.cc \
thirdparty/installed/include/easylogging++.cc \
-l pthread -l hdfs -l jvm -l rdkafka -DELPP_THREAD_SAFE -DELPP_NO_DEFAULT_LOG_FILE

