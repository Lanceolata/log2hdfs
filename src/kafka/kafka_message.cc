// Copyright (c) 2017 Lanceolata

#include "kafka/kafka_message.h"

namespace log2hdfs {

std::unique_ptr<KafkaMessage> KafkaMessage::Init(
    rd_kafka_message_t* rkmessage) {
  if (!rkmessage)
    return nullptr;
  return std::unique_ptr<KafkaMessage>(new KafkaMessage(rkmessage));
}

MessageTimestamp KafkaMessage::Timestamp() const {
  MessageTimestamp ts;
  rd_kafka_timestamp_type_t tstype;
  ts.timestamp = rd_kafka_message_timestamp(rkmessage_, &tstype);
  ts.type = static_cast<MessageTimestamp::MessageTimestampType>(tstype);
  return ts;
}

}   // namespace log2hdfs
