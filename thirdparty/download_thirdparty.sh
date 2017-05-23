#!/usr/bin/env bash

set -x
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

source $TP_DIR/versions.sh

download_extract_and_cleanup() {
    filename=$TP_DIR/$(basename "$1")
    wget -O $filename $1
    tar xzf $filename -C $TP_DIR
    rm $filename
}

download_extract_and_cleanup_bz2() {
    filename=$TP_DIR/$(basename "$1")
    wget -O $filename $1
    tar xjf $filename -C $TP_DIR
    rm $filename
}

if [ ! -d ${LIBRDKAFKA_BASEDIR} ]; then
    echo "Fetching librdkafka..."
    download_extract_and_cleanup $LIBRDKAFKA_URL
fi

#if [ ! -d ${JEMALLOC_BASEDIR} ]; then
#    echo "Fetching jemalloc..."
#    download_extract_and_cleanup_bz2 $JEMALLOC_URL
#fi

if [ ! -d ${GOOGLETEST_BASEDIR} ]; then
    echo "Fetching googletest..."
    download_extract_and_cleanup $GOOGLETEST_URL
fi

if [ ! -d ${EASYLOGGINGPP_BASEDIR} ]; then
    echo "Fetching easyloggingpp..."
    download_extract_and_cleanup $EASYLOGGINGPP_URL
fi
