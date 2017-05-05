#include <stdlib.h>
#include <unistd.h>
#include <string>
#include "log2kafka/log2kafka_errmsg_handle.h"
#include "log2kafka/log2kafka_offset_table.h"
#include "log2kafka/log2kafka_topic_conf.h"
#include "log2kafka/log2kafka_produce.h"
#include "log2kafka/log2kafka_inotify.h"
#include "kafka/kafka_producer.h"
#include "util/configparser.h"
#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP
using namespace log2hdfs;

static bool running = true;
static char *conf_path = NULL;
static std::shared_ptr<Log2kafkaErrmsgHandle> handle;
static std::shared_ptr<Log2kafkaOffsetTable> table;
static std::unordered_map<std::string,
    std::shared_ptr<Log2kafkaTopicConf>> topic_confs;
static std::unique_ptr<Log2kafkaProduce> produce;
static std::unique_ptr<Log2kafkaInotify> inotify;

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
  char* log_conf_path = NULL;

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
  el::Configurations log_conf("log_conf_path");
  el::Loggers::reconfigureAllLoggers(log_conf);
  el::Helpers::installPreRollOutCallback(rolloutHandler);

  // Init configuration
  std::shared_ptr<IniConfigParser> conf = IniConfigParser::Init();
  if (!conf->Read(conf_path)) {
     LOG(ERROR) << "IniConfigParser Read config file failed";
     exit(EXIT_FAILURE);
  }

  for (auto it1 = conf->Begin(); it1 != conf->End(); ++it1) {
    std::cerr << "[" << it1->first << "]" << std::endl;
    for (auto it2 = it1->second->Begin(); it2 != it1->second->End(); ++it2) {
      std::cerr << it2->first << " = " << it2->second << std::endl;
    }
    std::cerr << std::endl;
  }

  // Init thread safe queue
  std::shared_ptr<Queue<std::string>> queue = Queue<std::string>::Init();
  if (!queue) {
    LOG(ERROR) << "Init thread safe queue failed";
    exit(EXIT_FAILURE);
  }

  // Init Log2kafkaErrmsgHandle
  std::shared_ptr<Section> section_global = conf->GetSection("global");
  if (!section_global) {
    LOG(WARNING) << "Get section[global] failed";
    exit(EXIT_FAILURE);
  }

  std::string handle_dir = section_global->Get("handle.dir", "remedy");
  std::string handle_interval = section_global->Get("handle.interval", "1800");
  handle = Log2kafkaErrmsgHandle::Init(handle_dir,
      atoi(handle_interval.c_str()), queue);
  if (!handle) {
    LOG(ERROR) << "Log2kafkaErrmsgHandle Init failed handle_dir["
               << handle_dir << "] handle_interval["
               << handle_interval << "]";
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
    LOG(WARNING) << "Get section[kafka] failed";
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

  // Init Log2kafkaOffsetTable
  std::string table_path = section_global->Get("table.path", "log2kafka_table");
  std::string table_interval = section_global->Get("table.interval", "30");
  table = Log2kafkaOffsetTable::Init(table_path, atoi(table_interval.c_str()));
  if (!table) {
    LOG(ERROR) << "Log2kafkaOffsetTable Init failed table_path["
               << table_path << "] table_interval[" << table_interval
               << "]";
    exit(EXIT_FAILURE);
  }

  // Init log2kafka default conf
  std::shared_ptr<Section> section_default = conf->GetSection("default");
  if (!section_default) {
    LOG(WARNING) << "Get section[global] failed";
  } else {
    if (!Log2kafkaTopicConf::UpdataDefaultConf(section_global)) {
      LOG(ERROR) << "Log2kafkaTopicConf UpdataDefaultConf failed";
      exit(EXIT_FAILURE);
    }
  }

  // Init log2kafka topic confs
  for (auto it = conf->Begin(); it != conf->End(); ++it) {
    if (it->first == "global" || it->first == "kafka" || it->first == "default")
      continue;

    std::shared_ptr<Log2kafkaTopicConf> topic_conf =
        Log2kafkaTopicConf::Init(it->first);
    if (!topic_conf) {
      LOG(ERROR) << "Log2kafkaTopicConf Init topic:" << it->first
                 << " failed";
      exit(EXIT_FAILURE);
    }

    if (!topic_conf->InitConf(it->second)) {
      LOG(ERROR) << "Log2kafkaTopicConf InitConf topic:" << it->first
                 << " failed";
      exit(EXIT_FAILURE);
    }
    topic_confs[it->first] = topic_conf;
  }

  // Init log2kafka produce
  produce = Log2kafkaProduce::Init(std::move(producer), queue, table, handle);
  if (!produce) {
    LOG(ERROR) << "Log2kafkaProduce Init failed";
    exit(EXIT_FAILURE);
  }

  // Init log2kafka inotify
  inotify = Log2kafkaInotify::Init(queue, table);
  if (!inotify) {
    LOG(ERROR) << "Log2kafkaInotify Init failed";
    exit(EXIT_FAILURE);
  }

  // Add topic
  for (auto it = topic_confs.begin(); it != topic_confs.end(); ++it) {
    if (!produce->AddTopic(it->second)) {
      LOG(ERROR) << "Log2kafkaProduce AddTopic failed topic["
                 << it->first << "]";
      exit(EXIT_FAILURE);
    }
    if (!inotify->AddWatchTopic(it->second)) {
      LOG(ERROR) << "Log2kafkaInotify AddWatchTopic failed topic["
                 << it->first << "]";
      exit(EXIT_FAILURE);
    }
  }

  // Create thread
  handle->Start();
  table->Start();
  produce->Start();
  inotify->Start();

//  while (running) {
    pause();
//  }

  produce->Stop();
  table->Stop();
  el::Helpers::uninstallPreRollOutCallback();
  return 0;
}
