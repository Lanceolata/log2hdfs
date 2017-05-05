GTEST='/home/lanceolata/github/persional/log2hdfs/thirdparty/installed/lib/libgtest.a'
ELPP='/home/lanceolata/github/persional/log2hdfs/thirdparty/installed/include/easylogging++.cc'
g++ -std=c++11 main.cc */*.cc ../src/util/*.cc $ELPP $GTEST -lpthread -DELPP_THREAD_SAFE -DELPP_NO_DEFAULT_LOG_FILE
