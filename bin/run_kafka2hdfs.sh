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
export LD_LIBRARY_PATH="../log2hdfs/thirdparty/installed/lib:/usr/java/jdk1.8.0_77/jre/lib/amd64/server:/usr/hdp/2.4.0.0-169/usr/lib:/usr/hdp/2.4.0.0-169/hadoop/lib/native:$LD_LIBRARY_PATH"
export CLASSPATH=$classpaths
export HADOOP_OPTS="$HADOOP_OPTS -Djava.library.path=/usr/hdp/2.4.0.0-169/hadoop/lib/native -Djava.library.path=/usr/hdp/2.4.0.0-169/hadoop/lib"
export HADOOP_COMMON_LIB_NATIVE_DIR=/usr/hdp/2.4.0.0-169/hadoop/lib/native

type=$1

array=("v6" "report")

for i in ${array[@]}
do
    if [ "$type" == "$i" ]
    then
        valid=1
    fi
done

if [ ! -n "$valid" ]
then
    echo "unknown type:$type"
    exit 1
fi

procs=$(ps -ef | grep 'kafka2hdfs' | grep -v 'grep')

if echo "$procs" | grep -q "$type.conf"
then
    exit 0
fi

YmdHMs=$(date +%Y%m%d%H%M%S)
echo "[$YmdHMs]kafka2hdfs $type crashed or aborted"

../log2hdfs/bin/kafka2hdfs -c $type.conf -l $type-log.conf >> $type-stderr.log 2>&1 &