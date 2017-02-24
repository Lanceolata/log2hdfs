#include "util/queue.h"
#include <iostream>

int main() {
    std::shared_ptr<log2hdfs::Queue<int> > q(new log2hdfs::Queue<int>());
    q->Push(1);
    auto res = q->WaitPop();
    std::cout << *res << std::endl;
}
