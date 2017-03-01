#!/usr/bin/env bash

set -x
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

source $TP_DIR/versions.sh
PREFIX=$TP_DIR/installed

########################################################

if [ "$#" = "0" ]; then
    F_ALL=1
else
    # Allow passing specific libs to build on the command line
    for arg in "$@"; do
        case $arg in
            "librdkafka")       F_LIBRDKAFKA=1 ;;
            "jemalloc")         F_JEMALLOC=1 ;; 
            "googletest")       F_GOOGLETEST=1 ;;
            "easyloggingpp")    F_EASYLOGGINGPP=1 ;;
            *)              echo "Unknown module: $arg"; exit 1 ;;
        esac
    done
fi

########################################################

mkdir -p "$PREFIX/include"
mkdir -p "$PREFIX/lib"

# build librdkafka
if [ -n "$F_ALL" -o -n "$F_LIBRDKAFKA" ]; then
    cd $TP_DIR/$LIBRDKAFKA_BASEDIR
    ./configure --prefix=$PREFIX
    make
    make install
    cd $TP_DIR
    rm -rf $TP_DIR/$LIBRDKAFKA_BASEDIR
fi

# build jemalloc
if [ -n "$F_ALL" -o -n "$F_JEMALLOC" ]; then
    cd $TP_DIR/$JEMALLOC_BASEDIR
    ./configure --prefix=$PREFIX
    make
    make install
fi

# build googletest
if [ -n "$F_ALL" -o -n "$F_GOOGLETEST" ]; then
    cd $TP_DIR/$GOOGLETEST_BASEDIR
    CXXFLAGS=-fPIC cmake -DCMAKE_INSTALL_PREFIX=$PREFIX .
    make
    make install
fi

# build easyloggingpp
if [ -n "$F_ALL" -o -n "F_EASYLOGGINGPP" ]; then
    cd $TP_DIR/$EASYLOGGINGPP_BASEDIR
    cmake -DCMAKE_INSTALL_PREFIX=$PREFIX .
    make
    make install
fi
