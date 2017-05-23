#!/usr/bin/env bash

export LD_LIBRARY_PATH="../thirdparty/installed/lib:$LD_LIBRARY_PATH"

./log2kafka -c ../conf/log2kafka_conf_example.conf -l ../conf/log2kafka_logconfig_example.conf >> stderr.log 2>&1 &
