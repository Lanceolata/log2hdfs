#include <iostream>
#include <memory>
#include "configparser.h"

using namespace log2hdfs::util;

int main() {

    std::string filepath("/home/lanceolata/workspace/sample.ini");
    //std::string filepath("test.log");

    //auto conf = ConfigParser::KvConfig();
    //conf->read(filepath);
    //std::pair<OptionConstIter, OptionConstIter> pa = conf->options();
    //for (OptionConstIter it = pa.first; it != pa.second; it++) {
    //    std::cout << it->first << " = " << it->second << std::endl;
    //}

    //auto conf2 = ConfigParser::KvConfig();

    //*conf2 = *conf;
    //conf.reset();
    //pa = conf2->options();
    //for (OptionConstIter it = pa.first; it != pa.second; it++) {
    //  std::cout << it->first << " = " << it->second << std::endl;
    //}

    //return 0;
    auto conf = ConfigParser::IniConfig();
    conf->read(filepath);
    std::pair<SectionConstIter, SectionConstIter> p = conf->sections();
    for (SectionConstIter it = p.first; it != p.second; it++) {
        std::cout << "section:" << it->first << std::endl;
        std::pair<OptionConstIter, OptionConstIter> sp = (it->second).options();
        for (OptionConstIter oci = sp.first; oci != sp.second; oci++) {
            std::cout << oci->first << " = " << oci->second << std::endl;
        }
    }
    std::cout << "---------------------------------------------" << std::endl;
    std::cout << conf->has_section("common") << std::endl;
    std::cout << conf->has_section("111") << std::endl;
    std::cout << "---------------------------------------------" << std::endl;

    std::cout << conf->add_section("ftp") << std::endl;
    std::cout << conf->add_section("111") << std::endl;
    std::cout << conf->has_section("111") << std::endl;
    std::cout << "---------------------------------------------" << std::endl;

    std::cout << conf->remove_section("222") << std::endl;
    std::cout << conf->remove_section("111") << std::endl;
    std::cout << conf->has_section("111") << std::endl;
    std::cout << conf->has_option("ftp", "path") << std::endl;
    std::cout << conf->has_option("ftp", "qq") << std::endl;
    std::cout << conf->remove_option("ftp", "name") << std::endl;
    std::cout << conf->remove_option("ftp", "qq") << std::endl;
    std::cout << "---------------------------------------------" << std::endl;

    std::cout << *(conf->get("ftp", "path")) << std::endl;
    if ((conf->get("aaa", "aaa")) == nullptr) {
        std::cout << "nullptr" << std::endl;        
    }

    conf->set("ftp", "path", "value");
    std::cout << *(conf->get("ftp", "path")) << std::endl;
    std::cout << conf->set("aaa", "path", "value") << std::endl;

    std::cout << "---------------------------------------------" << std::endl;
    conf->write("test.log");
    auto conf2 = ConfigParser::IniConfig();
    *conf2 = *conf;
    conf.reset();
    p = conf2->sections();
    for (SectionConstIter it = p.first; it != p.second; it++) {
      std::cout << "section:" << it->first << std::endl;
      std::pair<OptionConstIter, OptionConstIter> sp = (it->second).options();
      for (OptionConstIter oci = sp.first; oci != sp.second; oci++) {
        std::cout << oci->first << " = " << oci->second << std::endl;
      }
    }
    return 0;
}
