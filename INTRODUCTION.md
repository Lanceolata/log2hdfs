log2hdfs由4个模块组成(详细代码建src目录下)：

## log2kafka

收集前端生成日志，produce到kafka。

主要由四个部分组成：

1. inotify：调用Linux inotify监控日志生成目录，处理新日志的生成、新目录的建立和监控、新增目录递归监控，目录移除。对于新生成日志文件，会通过队列给produce处理(格式为topic:path:offset)。

    为实现目录的递归监控，对于新建立的目录，等待目录3秒内不再修改，监控目录，记录监控事件，处理目录下的文件及目录。

2. produce：读取队列信息，将文件内容发送到指定topic，并更新offset_table中对应的offset信息。

3. offset_table：offset信息根据interval配置写入到本地文件，便于程序重启后恢复。

4. errmsg_handle：处理发送失败和time out的数据信息，按不同topic写到本地文件，根据interval配置更新本地文件，根据handle.remedy配置决定本地文件是否重新提交队列发送。(注:进程重启后本地的失败文件不再生效，即使配置handle.remedy = true也不会提交队列)


```
    graph TD
    A-->B
```

## kafka2hdfs

消费kafka数据，按业务时间归档，压缩并上传到hdfs指定路径。

主要由5个部分组成：

1. log_format：日志格式接口，便于扩展处理的日志格式

2. path_format：路径格式化接口，构造本地路径、hdfs路径等

3. consume_callback：kafka consume callback接口，处理consume message

4. upload：压缩和上传接口，根据文件格式匹配不同的压缩和上传规则

5. hdfs_handle：hdfs client 接口


## kafka

封装librdkafka

## util

工具模块：Ini conf parser、文件指针缓存、Optional、线程安全队列、system工具方法、string工具方法、线程池、