// Copyright (c) 2017 Lanceolata

#include <unistd.h>
#include <signal.h>
#include "log2kafka/errmsg_handle.h"
#include "log2kafka/offset_table.h"
#include "log2kafka/topic_conf.h"
#include "log2kafka/produce.h"
#include "log2kafka/inotify.h"
#include "kafka/kafka_producer.h"
#include "util/configparser.h"
#include "easylogging++.h"

// Init logging
INITIALIZE_EASYLOGGINGPP

using namespace log2hdfs;

// Running flag
static bool running = true;
// conf file path
static char *conf_path = NULL;

static std::shared_ptr<ErrmsgHandle> handle;
static std::shared_ptr<OffsetTable> table;
static std::unordered_map<std::string,
    std::shared_ptr<TopicConf>> topic_confs;
static std::unique_ptr<Produce> produce;
static std::unique_ptr<Inotify> inotify;

// logging roll handler
void RolloutHandler(const char* filename, std::size_t size) {
  std::stringstream stream;
  stream << filename << "." << time(NULL);
  rename(filename, stream.str().c_str());
}

// kafka err callback
void err_cb(rd_kafka_t* rk, int err, const char* reason, void* opaque) {
  LOG(WARNING) << "rdkafka error cb name:" << rd_kafka_name(rk)
               << " err:" << rd_kafka_err2str((rd_kafka_resp_err_t)err)
               << " reason:" << reason;
}

// kafka deliver message callback
void dr_msg_cb(rd_kafka_t* rk, const rd_kafka_message_t* rkmessage,
               void* opaque) {
  if (rkmessage->err == 0)
    return;

  // handle produce failed message
  const std::string topic = rd_kafka_topic_name(rkmessage->rkt);
  char *payload = static_cast<char *>(rkmessage->payload);
  size_t len = rkmessage->len;

  std::string msg(payload, len);
  if (handle) {
    handle->ArchiveMsg(topic, msg);
    LOG(WARNING) << "dr_msg_cb topic[" << topic << "] partition["
                 << rkmessage->partition << "] failed with error["
                 << rd_kafka_err2str(rkmessage->err) << "]";
  } else {
    LOG(WARNING) << "dr_msg_cb topic[" << topic << "] msg[" << payload << "]";
  }
}

// signals handler
void signals_handler(int sig) {
  if (sig != SIGUSR1) {
    LOG(INFO) << "handle_sigs existing";
    running = false;
    return;
  }

  std::shared_ptr<IniConfigParser> conf = IniConfigParser::Init();
  if (!conf->Read(conf_path)) {
    LOG(ERROR) << "signals_handler IniConfigParser Read config file failed";
    return;
  }

  for (auto it = conf->Begin(); it != conf->End(); ++it) {
    const std::string topic = it->first;
    if (topic == "global" || topic == "kafka" || topic == "default")
      continue;

    auto it2 = topic_confs.find(topic);
    if (it2 != topic_confs.end()) {
      if (!it2->second->UpdateRuntime(it->second)) {
        LOG(WARNING) << "signals_handler UpdateRuntime topic[" << topic
                     << "] failed";
      }
      continue;
    }

    std::shared_ptr<TopicConf> topic_conf = TopicConf::Init(it->first);
    if (!topic_conf) {
      LOG(WARNING) << "signals_handler TopicConf Init topic[" << topic
                   << "] failed";
      continue;
    }

    if (!topic_conf->InitConf(it->second)) {
      LOG(WARNING) << "signals_handler TopicConf InitConf topic[" << topic
                   << "] failed";
      continue;
    }

    if (!produce->AddTopic(topic_conf)) {
      LOG(WARNING) << "signals_handler Produce AddTopic topic[" << topic
                   << "] failed";
      continue;
    }

    if (!inotify->AddWatchTopic(topic_conf)) {
      LOG(WARNING) << "signals_handler Inotify AddWatchTopic topic[" << topic
                   << "] failed";
      produce->RemoveTopic(topic);
      continue;
    }

    topic_confs[topic] = topic_conf;
  }

  for (auto it = topic_confs.begin(); it != topic_confs.end();) {
    const std::string topic = it->first;
    if (conf->HasSection(topic)) {
      ++it;
    } else {
      inotify->RemoveWatchTopic(topic);
      produce->RemoveTopic(topic);
      topic_confs.erase(it++);
    }
  }
}

void set_signal_handlers() {
  signal(SIGTERM, signals_handler);
  signal(SIGINT, signals_handler);
  signal(SIGUSR1, signals_handler);
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

  // Init easylogging++
  el::Loggers::addFlag(el::LoggingFlag::StrictLogFileSizeCheck);
  el::Configurations log_conf(log_conf_path);
  el::Loggers::reconfigureAllLoggers(log_conf);
  el::Helpers::installPreRollOutCallback(RolloutHandler);

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
  std::shared_ptr<Section> global_section = conf->GetSection("global");
  if (!global_section) {
    LOG(ERROR) << "Get section[global] failed";
    exit(EXIT_FAILURE);
  }

  handle = ErrmsgHandle::Init(global_section, queue);
  if (!handle) {
    LOG(ERROR) << "ErrmsgHandle Init failed";
    exit(EXIT_FAILURE);
  }

  // Init offset table
  table = OffsetTable::Init(global_section);
  if (!table) {
    LOG(ERROR) << "OffsetTable Init failed";
    exit(EXIT_FAILURE);
  }

  // Init kafka producer global conf
  std::string errstr;
  std::unique_ptr<KafkaGlobalConf> producer_conf = KafkaGlobalConf::Init();
  if (!producer_conf) {
    LOG(ERROR) << "KafkaGlobalConf Init producer global conf failed";
    exit(EXIT_FAILURE);
  }

  std::shared_ptr<Section> kafka_section = conf->GetSection("kafka");
  if (!kafka_section) {
    LOG(ERROR) << "Get section[kafka] failed";
    exit(EXIT_FAILURE);
  }

  for (auto it = kafka_section->Begin(); it != kafka_section->End(); ++it) {
    if (producer_conf->Set(it->first, it->second, &errstr)
            == KafkaConfResult::kConfOk) {
      LOG(INFO) << "Set configuration name[" << it->first << "] value["
                << it->second << "] success";
    } else {
      LOG(ERROR) << "Set configuration name[" << it->first << "] value["
                 << it->second << "] failed with error[" << errstr << "]";
      exit(EXIT_FAILURE);
    }
  }

  producer_conf->SetErrorCb(err_cb);
  producer_conf->SetDrMsgCb(dr_msg_cb);

  // Init kafka producer
  std::shared_ptr<KafkaProducer> producer = KafkaProducer::Init(
      producer_conf.get(), &errstr);
  if (!producer) {
    LOG(ERROR) << "KafkaProducer Init failed with error[" << errstr << "]";
    exit(EXIT_FAILURE);
  }

  // Init log2kafka default conf
  std::shared_ptr<Section> default_section = conf->GetSection("default");
  if (!default_section) {
    LOG(WARNING) << "Get section[default] failed";
  } else {
    if (!TopicConf::UpdataDefaultConf(default_section)) {
      LOG(ERROR) << "TopicConf UpdataDefaultConf failed";
      exit(EXIT_FAILURE);
    }
  }

  // Init log2kafka topic confs
  for (auto it = conf->Begin(); it != conf->End(); ++it) {
    const std::string topic = it->first;
    if (topic == "global" || topic == "kafka" || topic == "default")
      continue;

    std::shared_ptr<TopicConf> topic_conf = TopicConf::Init(topic); 
    if (!topic_conf) {
      LOG(ERROR) << "TopicConf Init topic[" << topic << "] failed";
      exit(EXIT_FAILURE);
    }

    if (!topic_conf->InitConf(it->second)) {
      LOG(ERROR) << "TopicConf InitConf topic[" << topic << "] failed";
      exit(EXIT_FAILURE);
    }
    topic_confs[topic] = std::move(topic_conf);
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

  set_signal_handlers();

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
  return 0;
}
