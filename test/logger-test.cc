
#include <memory>

#include "logger.h"

using namespace log2hdfs;

int main() {
    std::string log_path = "log.log";
    
    auto logger = util::Logger::Create(log_path);

    logger->Error("11111111:%d", 11);

    logger->Warn("11111111:%d", 22);

    logger->Info("11111111:%d", 33);

    util::LogInit(nullptr);

    util::Log(util::LogLevel::kLogError, "fqffa%d:%sregewgew", 11, "QWERT");

    return 0;
}
