#include "tools.h"

#include <iostream>

int main()
{
    std::string a = "   111111  ";
    log2hdfs::util::trim(a);
    std::cout << a << ":trim" << std::endl;

    std::string b = " \n\n\n\ni222\r\r\r";
    std::cout << log2hdfs::util::trim(b) << ":trim" << std::endl;

    std::string c = "";
    std::cout << log2hdfs::util::trim(c) << std::endl;
    if (c.empty())
        std::cout << "empty" << std::endl;

    return 0;
}
