#!/usr/bin/env bash

# old
OLD_HDFS_INCLUDE="/usr/hdp/2.4.0.0-169/usr/include/"
OLD_HDFS_LIB="/usr/lib/hadoop/lib/native"
OLD_JAVA_LIB="/usr/jdk64/jdk1.7.0_45/jre/lib/amd64/server"

# new
HDFS_INCLUDE="/usr/hdp/2.4.0.0-169/usr/include/"
HDFS_LIB="/usr/hdp/2.4.0.0-169/usr/lib"
JAVA_LIB="/usr/java/jdk1.8.0_77/jre/lib/amd64/server"

g++ -g  -std=c++11 \
-I src \
-I thirdparty/installed/include \
-I $OLD_HDFS_INCLUDE \
-I $HDFS_INCLUDE \
-L thirdparty/installed/lib \
-L $OLD_JAVA_LIB \
-L $JAVA_LIB \
-L $OLD_HDFS_LIB \
-L $HDFS_LIB \
-o bin/kafka2hdfs src/kafka2hdfs/*.cc src/kafka/*.cc src/util/*.cc \
thirdparty/installed/include/easylogging++.cc \
-l pthread -l hdfs -l jvm -l rdkafka -DELPP_THREAD_SAFE -DELPP_NO_DEFAULT_LOG_FILE
