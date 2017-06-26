#!/usr/bin/env bash

#concat_jar_paths()
#{
#    str=$1
#    arr=(${str//:/ })
#    paths=
#    for s in ${arr[*]}
#    do
#        paths="$paths:$s"
#    done
#    echo $paths
#}

#classpaths=$(hadoop classpath)
#classpaths=$(concat_jar_paths $classpaths)

export LD_LIBRARY_PATH="../thirdparty/installed/lib:/usr/java/jdk1.8.0_77/jre/lib/amd64/server:/usr/hdp/2.4.0.0-169/usr/lib:/usr/hdp/2.4.0.0-169/hadoop/lib/native:$LD_LIBRARY_PATH"
export CLASSPATH=$classpaths
export HADOOP_OPTS="$HADOOP_OPTS -Djava.library.path=/usr/hdp/2.4.0.0-169/hadoop/lib/native -Djava.library.path=/usr/hdp/2.4.0.0-169/hadoop/lib"
export HADOOP_COMMON_LIB_NATIVE_DIR=/usr/hdp/2.4.0.0-169/hadoop/lib/native

./kafka2hdfs -c ../conf/kafka2hdfs_config_example.conf -l ../conf/log2kafka_logconfig_example.conf >> stderr.log 2>&1 &
