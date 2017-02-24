#include "util/system_utils.h"
#include <iostream>

int main() {
    log2hdfs::Optional<std::vector<std::string> > res =
        log2hdfs::ScanDirFile("/home/lanceolata/workspace", NULL, NULL);

    if (res.valid()) {
        for (std::string name : res.value()) {
            std::cout << name << std::endl;
        }
    } else {
        std::cout << errno << std::endl;
    }

    std::cout << log2hdfs::MakeDir("/home/lanceolata/workspace/testdir") << std::endl;
}
