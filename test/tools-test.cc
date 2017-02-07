#include "tools.h"
#include <string>
#include <iostream>

int main()
{
    std::string a = "   111111  ";
    log2hdfs::util::Trim(&a);
    std::cout << a << ":trim" << std::endl;

    std::string b = " \n\n\n\ni222\r\r\r";
    log2hdfs::util::Trim(&b);
    std::cout << b << ":trim" << std::endl;

    std::string c = "";
    log2hdfs::util::Trim(&c);
    std::cout << c << std::endl;
    if (c.empty())
        std::cout << "empty" << std::endl;

    return 0;
}
