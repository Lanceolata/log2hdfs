#!/usr/bin/env bash

# old
HDFS_INCLUDE="/usr/hdp/2.4.0.0-169/usr/include/"
HDFS_LIB="/usr/lib/hadoop/lib/native"
JAVA_LIB="/usr/jdk64/jdk1.7.0_45/jre/lib/amd64/server"

# new
#HDFS_INCLUDE="/usr/hdp/2.4.0.0-169/usr/include/"
#HDFS_LIB="/usr/hdp/2.4.0.0-169/usr/lib"
#JAVA_LIB="/usr/java/jdk1.8.0_77/jre/lib/amd64/server"

g++ -g  -std=c++11 \
-I src \
-I thirdparty/installed/include \
-I $HDFS_INCLUDE \
-L thirdparty/installed/lib \
-L $JAVA_LIB \
-L $HDFS_LIB \
-o bin/kafka2hdfs src/kafka2hdfs/*.cc src/kafka/*.cc src/util/*.cc \
thirdparty/installed/include/easylogging++.cc \
-l pthread -l hdfs -l jvm -l rdkafka -DELPP_THREAD_SAFE -DELPP_NO_DEFAULT_LOG_FILE
