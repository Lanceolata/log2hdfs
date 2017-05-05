#!/usr/bin/env bash

g++ -g -W -Wall -std=c++11 \
-I /data/users/data-infra/log2hdfs/src \
-I /data/users/data-infra/log2hdfs/thirdparty/installed/include \
-I /usr/hdp/2.4.0.0-169/usr/include/ \
-L /data/users/data-infra/log2hdfs/thirdparty/installed/lib \
-L /usr/java/jdk1.8.0_77/jre/lib/amd64/server \
-L /usr/hdp/2.4.0.0-169/usr/lib \
-o kafka2hdfs src/kafka2hdfs.cc src/kafka2hdfs/*.cc \
src/util/*.cc src/kafka/*.cc \
-l pthread -l hdfs -l jvm -l hdfs -l rdkafka

concat_jar_paths()
{
    str=$1
    arr=(${str//:/ })
    paths=
    for s in ${arr[*]}
    do
        paths="$paths:$s"
    done
    echo $paths
}

classpaths=$(hadoop classpath)
classpaths=$(concat_jar_paths $classpaths)

export LD_LIBRARY_PATH="/data/users/data-infra/log2hdfs/thirdparty/installed/lib:/usr/java/jdk1.8.0_77/jre/lib/amd64/server:/usr/hdp/2.4.0.0-169/usr/lib:/usr/hdp/2.4.0.0-169/hadoop/lib/native:$LD_LIBRARY_PATH"
export CLASSPATH=$classpaths
#export HADOOP_OPTS="$HADOOP_OPTS -Djava.library.path=/usr/hdp/2.4.0.0-169/hadoop/lib/native -Djava.library.path=/usr/hdp/2.4.0.0-169/hadoop/lib"
#export HADOOP_COMMON_LIB_NATIVE_DIR=/usr/hdp/2.4.0.0-169/hadoop/lib/native

./kafka2hdfs -c conf/kafka2hdfs_config_example.conf
