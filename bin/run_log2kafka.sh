#!/usr/bin/env bash

# crontab
# * * * * * cd /data/users/data-infra/log2kafka && sh run_log2kafka.sh $type >> run_log2kafka.log 2>&1

ulimit -c unlimited
export LD_LIBRARY_PATH="../log2hdfs/thirdparty/installed/lib:$LD_LIBRARY_PATH"

type=$1

array=("adp" "adp-unbid" "dsp" "dsp-aws" "stats" "stats-cm" "stats-cmo" "imp" "imp-v6" "cm" "sandbox")

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


procs=$(ps -ef | grep 'log2kafka' | grep -v 'grep')

if echo "$procs" | grep -q "$type.conf"
then
    exit 0
fi

YmdHMs=$(date +%Y%m%d%H%M%S)
echo "[$YmdHMs]log2kafka $type crashed or aborted"
../log2hdfs/bin/log2kafka -c $type.conf -l $type-log.conf > $type-stderr.log 2>&1 &
