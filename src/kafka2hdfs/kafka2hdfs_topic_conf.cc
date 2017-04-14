// Copyright (c) 2017 Lanceolata

#include "kafka2hdfs/kafka2hdfs_topic_conf.h"
#include "util/configparser.h"
#include "util/logger.h"
#include "util/system_utils.h"

namespace log2hdfs {

// ------------------------------------------------------------------
// K2HTopicConfContents 

const char default_rootdir[] = "/data/users/data-infra/kafka2hdfs";
const char default_lzo[] =
    "/usr/local/bin/lzop -1 -U -f --ignore-warn";
const char default_put[] = "hadoop fs -put";
const char default_append[] = "hadoop fs -appendToFile";
const char default_lzoindex[] =
    "hadoop jar /usr/hdp/2.4.0.0-169/hadoop/lib/"
    "hadoop-lzo-0.6.0.2.4.0.0-169.jar "
    "com.hadoop.compression.lzo.LzoIndexer";

K2HTopicConfContents::K2HTopicConfContents():
    kafka_topic_conf(KafkaTopicConf::Init(KafkaTopicConf::kConsumer)),
    rootdir(new std::string(default_rootdir)),
    consume_type(ConsumeCallback::kV6),
    consume_offset(RD_KAFKA_OFFSET_STORED),
    consume_interval(900),
    consume_maxsize(5368709120),
    consume_complete_interval(120),
    compress_type(Compress::kUnCompress),
    compress_interval(20),
    compress_lzo(new std::string(default_lzo)),
    upload_type(Upload::kText),
    upload_put(new std::string(default_put)),
    upload_append(new std::string(default_append)),
    upload_lzoindex(new std::string(default_lzoindex)) {}

K2HTopicConfContents::K2HTopicConfContents(const K2HTopicConfContents &other):
    kafka_topic_conf(other.kafka_topic_conf->Copy()),
    rootdir(other.rootdir),
    consume_type(other.consume_type),
    consume_offset(other.consume_offset),
    consume_interval(other.consume_interval.load()),
    consume_maxsize(other.consume_maxsize.load()),
    consume_complete_interval(other.consume_complete_interval.load()),
    compress_type(other.compress_type),
    compress_interval(other.compress_interval.load()),
    compress_lzo(other.compress_lzo),
    upload_type(other.upload_type),
    upload_put(other.upload_put),
    upload_append(other.upload_append),
    upload_lzoindex(other.upload_lzoindex) {}


// ------------------------------------------------------------------
// Kafka2hdfsTopicConf

bool Kafka2hdfsTopicConf::UpdateDefaultConf(
    std::shared_ptr<Section> section) {
   K2HTopicConfContents &con = Kafka2hdfsTopicConf::DEFAULT_CONTENTS;

   Optional<std::string> rootdir = section->Get("rootdir");
   if (rootdir.valid()) {
     con.rootdir = std::make_shared<std::string>(rootdir.value());
   }
   Log(LogLevel::kLogInfo, "Default rootdir[%s]", con.rootdir->c_str());
   return false;
}
/*
std::unique_ptr<Kafka2hdfsTopicConf> Kafka2hdfsTopicConf::Init(
    const std::string &section_name, std::shared_ptr<Section> section) {

}

Kafka2hdfsTopicConf::Kafka2hdfsTopicConf(
    const std::string &section_name, std::shared_ptr<Section> section) {

}
*/
}   // namespace log2hdfs
