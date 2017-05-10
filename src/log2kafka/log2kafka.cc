// Copyright (c) 2017 Lanceolata

#include <unistd.h>
#include "log2kafka/errmsg_handle.h"
#include "log2kafka/offset_table.h"
#include "log2kafka/topic_conf.h"
#include "log2kafka/produce.h"
#include "log2kafka/inotify.h"
#include "kafka/kafka_producer.h"
#include "util/configparser.h"
#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP
using namespace log2hdfs;

static bool running = true;
static char *conf_path = NULL;

static std::shared_ptr<ErrmsgHandle> handle;
static std::shared_ptr<OffsetTable> table;
static std::unordered_map<std::string,
    std::shared_ptr<TopicConf>> topic_confs;
static std::unique_ptr<Produce> produce;
static std::unique_ptr<Inotify> inotify;

void rolloutHandler(const char* filename, std::size_t size) {
  std::stringstream stream;
  stream << filename << "." << time(NULL);
  rename(filename, stream.str().c_str());
}

void dr_msg_cb(rd_kafka_t* rk, const rd_kafka_message_t* rkmessage,
               void* opaque) {
  if (rkmessage->err == 0)
    return;

  const std::string topic = rd_kafka_topic_name(rkmessage->rkt);
  char *payload = static_cast<char *>(rkmessage->payload);
  size_t len = rkmessage->len;

  std::string msg(payload, len);
  if (handle) {
    handle->ArchiveMsg(topic, msg);
  } else {
    LOG(WARNING) << "dr_msg_cb topic:" << topic << " msg[" << payload << "]";
  }
}

void err_cb(rd_kafka_t* rk, int err, const char* reason, void* opaque) {
  LOG(WARNING) << "rdkafka error cb name:" << rd_kafka_name(rk)
               << " err:" << rd_kafka_err2str((rd_kafka_resp_err_t)err)
               << " reason:" << reason;
}

int main(int argc, char *argv[]) {
  int opt;
  char *log_conf_path = NULL;

  while ((opt = getopt(argc, argv, "c:l:")) != -1) {
    switch (opt) {
      case 'c':
        conf_path = optarg;
        std::cerr << "Config path for log2kafka: " << conf_path << std::endl;
        break;
      case 'l':
        log_conf_path = optarg;
        std::cerr << "Log config path for log2kafka: " << log_conf_path
                  << std::endl;
        break;
      default:
        std::cerr << "Usage: ./log2kafka -c conf_path -l log_conf_path"
                  << std::endl;
    }
  }

  if (log_conf_path == NULL) {
    std::cerr << "Usage: ./log2kafka -c conf_path -l log_conf_path"
              << std::endl;
    exit(EXIT_FAILURE);
  }

  if (conf_path == NULL) {
    std::cerr << "Usage: ./log2kafka -c conf_path -l log_conf_path"
              << std::endl;
    exit(EXIT_FAILURE);
  }

  el::Loggers::addFlag(el::LoggingFlag::StrictLogFileSizeCheck);
  el::Configurations log_conf(log_conf_path);
  el::Loggers::reconfigureAllLoggers(log_conf);
  el::Helpers::installPreRollOutCallback(rolloutHandler);

  // Init configuration
  std::shared_ptr<IniConfigParser> conf = IniConfigParser::Init();
  if (!conf->Read(conf_path)) {
    LOG(ERROR) << "IniConfigParser Read config file failed";
    exit(EXIT_FAILURE);
  }

  // Init thread safe queue
  std::shared_ptr<Queue<std::string>> queue = Queue<std::string>::Init();
  if (!queue) {
    LOG(ERROR) << "Init thread safe queue failed";
    exit(EXIT_FAILURE);
  }

  // Init ErrmsgHandle
  std::shared_ptr<Section> section_global = conf->GetSection("global");
  if (!section_global) {
    LOG(ERROR) << "Get section[global] failed";
    exit(EXIT_FAILURE);
  }

  handle = ErrmsgHandle::Init(section_global, queue);
  if (!handle) {
    LOG(ERROR) << "ErrmsgHandle Init failed";
    exit(EXIT_FAILURE);
  }

  // Init offset table
  table = OffsetTable::Init(section_global);
  if (!table) {
    LOG(ERROR) << "OffsetTable Init failed";
    exit(EXIT_FAILURE);
  }

  // Init kafka producer global conf
  std::string errstr;
  std::unique_ptr<KafkaGlobalConf> producer_conf = KafkaGlobalConf::Init(
      KafkaGlobalConf::Type::kProducer);
  if (!producer_conf) {
    LOG(ERROR) << "KafkaGlobalConf Init producer global conf failed";
    exit(EXIT_FAILURE);
  }

  std::shared_ptr<Section> section_kafka = conf->GetSection("kafka");
  if (!section_kafka) {
    LOG(ERROR) << "Get section[kafka] failed";
    exit(EXIT_FAILURE);
  }

  for (auto it = section_kafka->Begin(); it != section_kafka->End(); ++it) {
    if (producer_conf->Set(it->first, it->second, &errstr)
            == KafkaConfResult::kConfOk) {
      LOG(INFO) << "Set configuration key[" << it->first << "] value["
                << it->second << "] successed";
    } else {
      LOG(ERROR) << "Set configuration key[" << it->first << "] value["
                 << it->second << "] failed with error:" << errstr;
      exit(EXIT_FAILURE);
    }
  }

  producer_conf->SetErrorCb(err_cb);
  producer_conf->SetDrMsgCb(dr_msg_cb);

  // Init kafka producer
  std::shared_ptr<KafkaProducer> producer = KafkaProducer::Init(
      producer_conf.get(), &errstr);
  if (!producer) {
    LOG(ERROR) << "KafkaProducer Init failed with error:" << errstr;
    exit(EXIT_FAILURE);
  }

  // Init log2kafka default conf
  std::shared_ptr<Section> section_default = conf->GetSection("default");
  if (!section_default) {
    LOG(WARNING) << "Get section[global] failed";
  } else {
    if (!TopicConf::UpdataDefaultConf(section_default)) {
      LOG(ERROR) << "TopicConf UpdataDefaultConf failed";
      exit(EXIT_FAILURE);
    }
  }

  // Init log2kafka topic confs
  for (auto it = conf->Begin(); it != conf->End(); ++it) {
    if (it->first == "global" || it->first == "kafka" || it->first == "default")
      continue;

    std::shared_ptr<TopicConf> topic_conf = TopicConf::Init(it->first);
    if (!topic_conf) {
      LOG(ERROR) << "TopicConf Init topic[" << it->first << "] failed";
      exit(EXIT_FAILURE);
    }

    if (!topic_conf->InitConf(it->second)) {
      LOG(ERROR) << "TopicConf InitConf topic[" << it->first << "] failed";
      exit(EXIT_FAILURE);
    }
    topic_confs[it->first] = topic_conf;
  }

  // Init log2kafka produce
  produce = Produce::Init(std::move(producer), queue, table, handle);
  if (!produce) {
    LOG(ERROR) << "Produce Init failed";
    exit(EXIT_FAILURE);
  }

  // Init log2kafka inotify
  inotify = Inotify::Init(queue, table);
  if (!inotify) {
    LOG(ERROR) << "Inotify Init failed";
    exit(EXIT_FAILURE);
  }

  // Add topic
  for (auto it = topic_confs.begin(); it != topic_confs.end(); ++it) {
    if (!produce->AddTopic(it->second)) {
      LOG(ERROR) << "Produce AddTopic topic[" << it->first << "] failed";
      exit(EXIT_FAILURE);
    }

    if (!inotify->AddWatchTopic(it->second)) {
      LOG(ERROR) << "Inotify AddWatchTopic topic[" << it->first << "] failed";
      exit(EXIT_FAILURE);
    }
  }

  // Create thread
  handle->Start();
  table->Start();
  produce->Start();
  inotify->Start();

  while (running) {
    pause();
  }

  // Stop thread
  produce->Stop();
  table->Stop();
  el::Helpers::uninstallPreRollOutCallback();
}
