// Copyright (c) 2017 Lanceolata

#include <unistd.h>
#include <signal.h>
#include "kafka/kafka_consumer.h"
#include "kafka2hdfs/hdfs_handle.h"
#include "kafka2hdfs/topic_conf.h"
#include "util/configparser.h"
#include "easylogging++.h"

// Init logging
INITIALIZE_EASYLOGGINGPP

using namespace log2hdfs;

// Stop flag
static bool stop = false;
// conf file path
static char *conf_path = NULL;

static std::shared_ptr<KafkaConsumer> consumer;
static std::shared_ptr<HdfsHandle> handle;
static std::unordered_map<std::string,
    std::shared_ptr<TopicConf>> topic_confs;
static std::unordered_map<std::string,
    std::unique_ptr<Upload>> topic_uploads;

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

// signals handler
void signals_handler(int sig) {
  if (sig != SIGUSR1) {
    LOG(INFO) << "handle_sigs existing";
    stop = true;
    return;
  }

  std::shared_ptr<IniConfigParser> conf = IniConfigParser::Init();
  if (!conf->Read(conf_path)) {
    LOG(ERROR) << "signals_handler IniConfigParser Read config file failed";
    return;
  }

  std::string errstr;
  for (auto it = conf->Begin(); it != conf->End(); ++it) {
    const std::string topic = it->first;
    if (topic == "global" || topic == "kafka" || topic == "default"
            || topic == "hdfs")
      continue;

    // handle running topic
    auto it2 = topic_confs.find(topic);
    if (it2 != topic_confs.end()) {
      if (!it2->second->UpdateRuntime(it->second)) {
        LOG(WARNING) << "signals_handler UpdateRuntime topic[" << topic
                     << "] failed";
      }
      continue;
    }

    // handle new topic
    std::shared_ptr<TopicConf> topic_conf = TopicConf::Init(topic);
    if (!topic_conf) {
      LOG(ERROR) << "signals_handler TopicConf Init topic[" << topic
                 << "] failed";
      continue;
    }

    if (!topic_conf->InitConf(it->second)) {
      LOG(ERROR) << "signals_handler TopicConf InitConf topic[" << topic
                 << "] failed";
      continue;
    }

    std::shared_ptr<FpCache> cache = FpCache::Init();
    if (!cache) {
      LOG(ERROR) << "signals_handler FpCache Init failed";
      continue;
    }

    std::shared_ptr<PathFormat> format = PathFormat::Init(topic_conf);
    if (!format) {
      LOG(ERROR) << "signals_handler PathFormat Init failed";
      continue;
    }

    std::shared_ptr<KafkaConsumeCb> cb = ConsumeCallback::Init(
        topic_conf, format, cache);
    if (!cb) {
      LOG(ERROR) << "signals_handler ConsumeCallback Init failed";
      continue;
    }

    std::unique_ptr<Upload> upload = Upload::Init(topic_conf,
        format, cache, handle);
    if (!upload) {
      LOG(ERROR) << "signals_handler Upload Init failed";
      continue;
    }

    if (!consumer->CreateTopicConsumer(
                topic,
                topic_conf->kafka_topic_conf().get(),
                topic_conf->partitions(),
                topic_conf->offsets(),
                cb, &errstr)) {
      LOG(ERROR) << "signals_handler CreateTopicConsumer topic[" << topic
                 << "] failed with errstr[" << errstr << "]";
      continue;
    }

    consumer->StartTopic(topic);
    upload->Start();

    topic_confs[topic] = std::move(topic_conf);
    topic_uploads[topic] = std::move(upload);
  }

  // remove topic
  for (auto it = topic_confs.begin(); it != topic_confs.end();) {
    const std::string topic = it->first;
    if (conf->HasSection(topic)) {
      ++it;
    } else {
      consumer->StopTopic(topic);
      topic_confs.erase(it++);
      topic_uploads[topic]->Join();
      topic_uploads.erase(topic);
    }
  }
  return;
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
        std::cerr << "Config path for kafka2hdfs: " << conf_path << std::endl;
        break;
      case 'l':
        log_conf_path = optarg;
        std::cerr << "Log config path for kafka2hdfs: " << log_conf_path
                  << std::endl;
        break;
      default:
        std::cerr << "Usage: ./kafka2hdfs -c conf_path -l log_conf_path"
                  << std::endl;
    }
  }

  if (log_conf_path == NULL) {
    std::cerr << "Usage: ./kafka2hdfs -c conf_path -l log_conf_path"
              << std::endl;
    exit(EXIT_FAILURE);
  }

  if (conf_path == NULL) {
    std::cerr << "Usage: ./kafka2hdfs -c conf_path -l log_conf_path"
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

  // Init hdfs handle
  std::shared_ptr<Section> hdfs_section = conf->GetSection("hdfs");
  if (!hdfs_section) {
    LOG(ERROR) << "Get section[hdfs] failed";
    exit(EXIT_FAILURE);
  }

  handle = HdfsHandle::Init(std::move(hdfs_section));
  if (!handle) {
    LOG(ERROR) << "HdfsHandle Init failed";
    exit(EXIT_FAILURE);
  }

  // Init kafka consumer global conf
  std::string errstr;
  std::unique_ptr<KafkaGlobalConf> consumer_conf = KafkaGlobalConf::Init();
  if (!consumer_conf) {
    LOG(ERROR) << "KafkaGlobalConf Init consumer global conf failed";
    exit(EXIT_FAILURE);
  }

  std::shared_ptr<Section> kafka_section = conf->GetSection("kafka");
  if (!kafka_section) {
    LOG(ERROR) << "Get section[kafka] failed";
    exit(EXIT_FAILURE);
  }

  for (auto it = kafka_section->Begin(); it != kafka_section->End(); ++it) {
    if (consumer_conf->Set(it->first, it->second, &errstr)
            == KafkaConfResult::kConfOk) {
      LOG(INFO) << "Set configuration name[" << it->first << "] value["
                << it->second << "] success";
    } else {
      LOG(ERROR) << "Set configuration name[" << it->first << "] value["
                 << it->second << "] failed with error[" << errstr << "]";
      exit(EXIT_FAILURE);
    }
  }
  consumer_conf->SetErrorCb(err_cb);

  // Init kafka consumer
  consumer = KafkaConsumer::Init(consumer_conf.get(), &errstr);
  if (!consumer) {
    LOG(ERROR) << "KafkaConsumer Init failed with error[" << errstr << "]";
    exit(EXIT_FAILURE);
  }

  // Init kafka2hdfs default conf
  std::shared_ptr<Section> default_section = conf->GetSection("default");
  if (!default_section) {
    LOG(WARNING) << "Get section[default] failed";
  } else {
    if (!TopicConf::UpdataDefaultConf(default_section)) {
      LOG(ERROR) << "TopicConf UpdataDefaultConf failed";
      exit(EXIT_FAILURE);
    }
  }

  // Init kafka2hdfs topic confs
  for (auto it = conf->Begin(); it != conf->End(); ++it) {
    const std::string topic = it->first;
    if (topic == "global" || topic == "kafka" || topic == "default"
            || topic == "hdfs")
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

    std::shared_ptr<FpCache> cache = FpCache::Init();
    if (!cache) {
      LOG(ERROR) << "FpCache Init failed";
      exit(EXIT_FAILURE);
    }

    std::shared_ptr<PathFormat> format = PathFormat::Init(topic_conf);
    if (!format) {
      LOG(ERROR) << "PathFormat Init failed";
      exit(EXIT_FAILURE);
    }

    std::shared_ptr<KafkaConsumeCb> cb = ConsumeCallback::Init(
        topic_conf, format, cache);
    if (!cb) {
      LOG(ERROR) << "ConsumeCallback Init failed";
      exit(EXIT_FAILURE);
    }

    std::unique_ptr<Upload> upload = Upload::Init(topic_conf,
        format, cache, handle);
    if (!upload) {
      LOG(ERROR) << "Upload Init failed";
      exit(EXIT_FAILURE);
    }

    if (!consumer->CreateTopicConsumer(
                topic,
                topic_conf->kafka_topic_conf().get(),
                topic_conf->partitions(),
                topic_conf->offsets(),
                cb, &errstr)) {
      LOG(ERROR) << "CreateTopicConsumer topic[" << topic
                 << "] failed with errstr[" << errstr << "]";
      exit(EXIT_FAILURE);
    }

    topic_confs[topic] = std::move(topic_conf);
    topic_uploads[topic] = std::move(upload);
  }

  set_signal_handlers();
  consumer->StartAllTopic();
  sleep(10);
  for (auto it = topic_uploads.begin(); it != topic_uploads.end(); ++it) {
    it->second->Start();
  }

  while (!stop) {
    pause();
  }

  consumer->StopAllTopic();
  for (auto it = topic_uploads.begin(); it != topic_uploads.end(); ++it) {
    it->second->Stop();
  }
  for (auto it = topic_uploads.begin(); it != topic_uploads.end(); ++it) {
    it->second->Join();
  }
  el::Helpers::uninstallPreRollOutCallback();
  return 0;
}
