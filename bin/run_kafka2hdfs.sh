#!/usr/bin/env bash

# crontab
# * * * * * cd /data/users/data-infra/kafka2hdfs && sh run_kafka2hdfs.sh $type >> run_kafka2hdfs.log 2>&1

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

ulimit -c unlimited

export LD_LIBRARY_PATH="../../log2hdfs/thirdparty/installed/lib:/usr/java/jdk1.8.0_77/jre/lib/amd64/server:/usr/jdk64/jdk1.7.0_45/jre/lib/amd64/server:/usr/hdp/2.4.0.0-169/usr/lib:/usr/hdp/2.4.0.0-169/hadoop/lib/native:/usr/lib/hadoop/lib/native:$LD_LIBRARY_PATH"
export CLASSPATH=$classpaths
export HADOOP_OPTS="$HADOOP_OPTS -Djava.library.path=/usr/hdp/2.4.0.0-169/usr/lib/native -Djava.library.path=/usr/hdp/2.4.0.0-169/usr/lib -Djava.library.path=/usr/lib/hadoop/lib/native -Djava.library.path=/usr/lib/hadoop/lib"
export HADOOP_COMMON_LIB_NATIVE_DIR=/usr/hdp/2.4.0.0-169/usr/lib/native

type=$1

procs=$(ps -ef | grep 'kafka2hdfs' | grep -v 'grep')

if echo "$procs" | grep -q "$type.conf"
then
    exit 0
fi

YmdHMs=$(date +%Y%m%d%H%M%S)
echo "[$YmdHMs]kafka2hdfs $type crashed or aborted"

../../log2hdfs/bin/kafka2hdfs -c $type.conf -l $type-log.conf >> $type-stderr.log 2>&1 &
