
#include "kafka/conf.h"
#include <iostream>

using namespace log2hdfs::kafka;

int main() {
    std::unique_ptr<GlobalConf> conf = GlobalConf::Create(GlobalConf::ConfType::kConfConsumer);

    std::string value;
    conf->Get("client.id", &value);
    std::cout << value << std::endl;

    std::cout << conf->Set("client.id", "consumer1", NULL) << std::endl;

    conf->Get("client.id", &value);
    std::cout << value << std::endl;


    std::unique_ptr<TopicConf> conf2 = TopicConf::Create(TopicConf::ConfType::kConfProducer);
    std::string error;

    std::cout << conf2->Set("client.id", "consumer1", &error) << std::endl;
    std::cout << error << std::endl;

    return 0;
}
