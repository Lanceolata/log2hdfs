
#include <memory>

#include "logger.h"

using namespace log2hdfs;

int main() {
    std::string log_path = "log.log";
    
    auto logger = util::SimpleLogger::Make(log_path);

    logger->error("11111111:%d", 11);

    logger->warning("11111111:%d", 22);

    logger->info("11111111:%d", 33);

    return 0;
}
