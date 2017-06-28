#!/usr/bin/env bash

export LD_LIBRARY_PATH="../log2hdfs/thirdparty/installed/lib:$LD_LIBRARY_PATH"

type=$1

if [ "$type" == "sandbox" ]
then
    conf="sandbox.conf"
    log="sandbox-log.conf"
elif [ "$type" == "dsp" ]
then
    conf="dsp.conf"
    log="dsp-log.conf"
elif [ "$type" == "dsp-test" ]
then
    conf="dsp-test.conf"
    log="dsp-test-log.conf"
elif [ "$type" == "adp" ]
then
    conf="adp.conf"
    log="adp-log.conf"
else
    echo "unknown type:$type"
    exit 1
fi

../log2hdfs/bin/log2kafka -c $conf -l $log >> $type-stderr.log 2>&1 &
