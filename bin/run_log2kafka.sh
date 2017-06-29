#!/usr/bin/env bash

ulimit -c unlimited
export LD_LIBRARY_PATH="../log2hdfs/thirdparty/installed/lib:$LD_LIBRARY_PATH"

type=$1

array=("adp" "adp-unbid" "dsp" "stats" "stats-cm" "stats-cmo" "imp" "imp-v6" "sandbox")

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

echo "../log2hdfs/bin/log2kafka -c $type.conf -l $type-log.conf > $type-stderr.log 2>&1 &"
