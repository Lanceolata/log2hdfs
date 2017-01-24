#include <iostream>
#include <memory>
#include "configparser.h"
#include "configparserimp.h"

using namespace log2hdfs::util;

int main() {
    std::string filepath("/home/lanceolata/workspace/sample.ini");
/*
    SectionImp si;

    std::cout << "separator:" << si.separator() << " " << "expect:=" << std::endl;
    si.set_separator(":");
    std::cout << "separator:" << si.separator() << " " << "expect::" << std::endl;
    si.set_separator("=");

    std::cout << "----------------------------------------------" << std::endl;

    si.Read(filepath);

    OptionConstIter cbegin, cend;
    for (cbegin = si.Begin(); cbegin != si.End(); cbegin++) {
        std::cout << cbegin->first << "<--->" << cbegin->second << std::endl;
    }

    std::cout << "----------------------------------------------" << std::endl;
    OptionIter begin, end;
    for (begin = si.Begin(); begin != si.End(); begin++) {
        std::cout << begin->first << "<--->" << begin->second << std::endl;
    }

    std::cout << "----------------------------------------------" << std::endl;
    si.Options(&cbegin, &cend);
    for (; cbegin != cend; cbegin++) {
        std::cout << cbegin->first << "<--->" << cbegin->second << std::endl;
    }
    
    std::cout << "----------------------------------------------" << std::endl;
    si.Options(&begin, &end);
    for (; begin != end; begin++) {
        std::cout << begin->first << "<--->" << begin->second << std::endl;
    }

    std::cout << "----------------------------------------------" << std::endl;
    
    std::cout << "has option[path]" << si.Has("path") << " " << "expect:1" << std::endl;
    std::cout << "has option[path]" << si.Has("") << " " << "expect:1" << std::endl;
    std::cout << "has option[path]" << si.Has("111") << " " << "expect:0" << std::endl;

    std::cout << "----------------------------------------------" << std::endl;
    std::cout << "remove option[path]" << si.Remove("path") << " " << "expect:1" << std::endl;
    std::cout << "remove option[path]" << si.Has("111") << " " << "expect:0" << std::endl;

    std::cout << "----------------------------------------------" << std::endl;
    std::cout << "get option[name]" << *(si.Get("name")) << " " << "expect:http uploading" << std::endl;
    std::cout << "set option[name]" << si.Set("name", "qq") << " " << "expect:1" << std::endl;
    std::cout << "get option[name]" << *(si.Get("name")) << " " << "expect:qq" << std::endl;
    if (si.Get("111") == NULL) {
        std::cout << "NULL" << std::endl;
    }

    std::cout << "----------------------------------------------" << std::endl;
    si.Write("/home/lanceolata/workspace/test1.conf");
    si.Write("/home/lanceolata/workspace/test2.conf", ":");

    SectionImp si2 = si;
    si.Remove("name");
    si2.Write("/home/lanceolata/workspace/test3.conf");
*/
   
    IniConfigParserImp icpi;
    icpi.Read(filepath);

    std::cout << "----------------------------------------------" << std::endl;
    SectionIter begin, end;
    for (begin = icpi.Begin(); begin != icpi.End(); begin++) {
        std::cout << "[" << begin->first << "]" << std::endl;
        OptionConstIter obegin, oend;
        for (obegin = (begin->second)->Begin(); obegin != (begin->second)->End(); obegin++) {
            std::cout << obegin->first << "<--->" << obegin->second << std::endl; 
        }
    }

    OptionIter obegin, oend;
    icpi.Options("ftp", &obegin, &oend);
    for (; obegin != oend; obegin++) {
        std::cout << obegin->first << "<--->" << obegin->second << std::endl;
    }

    std::cout << "----------------------------------------------" << std::endl;
    std::cout << "has section[ftp]" << icpi.HasSection("ftp") << " " << "expect:1" << std::endl;
    std::cout << "has section[qq]" << icpi.HasSection("qq") << " " << "expect:0" << std::endl;
    std::cout << "get option[name]" << *(icpi.Get("ftp", "name")) << " " << "expect:hello there, ftp uploading" << std::endl;

    IniConfigParserImp icpi2 = icpi;

    icpi.RemoveSection("http");
    icpi.RemoveOption("ftp", "name");
    icpi.Set("ftp", "path", "a");
    icpi.AddSection("test");

    icpi.Write("/home/lanceolata/workspace/test4.conf");
    icpi2.Write("/home/lanceolata/workspace/test5.conf");

    return 0;
}
