#include <string>
#include <iostream>
#include "util/string_utils.h"

int main()
{
    std::string a = "   111111  ";
    a = log2hdfs::TrimString(a);
    std::cout << a << ":trim" << std::endl;

    std::string b = " \n\n\n\ni222\r\r\r";
    b = log2hdfs::TrimString(b);
    std::cout << b << ":trim" << std::endl;

    std::string c = "";
    c = log2hdfs::TrimString(c);
    std::cout << c << std::endl;
    if (c.empty())
        std::cout << "empty" << std::endl;

    std::string e = "1234567890";
    std::string f = "90";
    std::cout << log2hdfs::EndsWith(e, f) << std::endl;
    return 0;
}
