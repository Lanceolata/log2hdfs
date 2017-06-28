#!/usr/bin/env bash

export LD_LIBRARY_PATH="../log2hdfs/thirdparty/installed/lib:$LD_LIBRARY_PATH"

type=$1

if [ "$type" == "adp" ]
then
    ../log2hdfs/bin/log2kafka -c adp.conf -l adp-log.conf > adp-stderr.log 2>&1 &
elif [ "$type" == "adp-unbid" ]
then
    ../log2hdfs/bin/log2kafka -c adp-unbid.conf -l adp-unbid-log.conf > adp-unbid-stderr.log 2>&1 &
elif [ "$type" == "dsp" ]
then
    ../log2hdfs/bin/log2kafka -c dsp.conf -l dsp-log.conf > dsp-stderr.log 2>&1 &
elif [ "type" == "stats" ]
then
    ../log2hdfs/bin/log2kafka -c stats.conf -l stats-log.conf > stats-stderr.log 2>&1 &
elif [ "type" == "stats-cm" ]
then
    ../log2hdfs/bin/log2kafka -c stats-cm.conf -l stats-cm-log.conf > stats-cm-stderr.log 2>&1 &
elif [ "type" == "stats-cmo" ]
then
    ../log2hdfs/bin/log2kafka -c stats-cmo.conf -l stats-cmo-log.conf > stats-cmo-stderr.log 2>&1 &
elif [ "$type" == "sandbox" ]
then
    ../log2hdfs/bin/log2kafka -c sandbox.conf -l sandbox.conf > sandbox-stderr.log 2>&1 &
else
    echo "unknown type:$type"
    exit 1
fi
