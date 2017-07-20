# log2kafka

## kafka configuration properties

see librdkafka configuration properties

注：老版kafka为0.8版本需要配置broker.version.fallback = 0.8.2(模板中已配好)

## Global configuration properties

Property | type | Range | Default | Description
---|---|---|---|---
handle.dir | string | | remedy | 发送失败和超时的messages写入目录
handle.interval | int | 60 - 2147483647 | 1800 | errmsg_handle更新文件时间间隔，文件更新后，会根据handle.remedy配置决定是否重新发送
handle.remedy | bool | true，false | false | errmsg_handle是否重新发送到kafka
table.path | string | | offset_table | offset持久化文件
table.interval | 1 - 2147483647 | 30 | offset持久化到文件的时间间隔

## Default configuration properties

Default配置对所有topic生效，可以在topic中覆盖default中的配置

Property | type | Range | Default | Description
---|---|---|---|---
remedy | int| -2147483647 - 2147483647 | 0 | 历史文件过期时间(s)，0不处理任何历史文件，小于0表示永不过期
batch.num | int | 1 - 2147483647 | 200 | produce每批次发送的message数量，发送后会更新offset信息
poll.timeout | int | 1 - 2147483647 | 300 | kafka client poll timeout，librdkafka poll函数，频繁调用
poll.messages | int | 1 - 2147483647 | 500 | kafka client队列满后，需要等待队列中message减少到的数量

可以配置librdkafka topic configuration properties，需要在配置前上'kafka.'

例如：配置message.timeout.ms = 60000，需要写为kafka.message.timeout.ms = 60000

注:重新启动时，log2kafka会根据remedy配置，过滤掉与当前时间相比，超过过期时间的文件，然后参会根据offset信息进一步过滤。
第一次启动时，如果不需要处理历史文件，建议配置为0(不处理历史文件)，如果需要处理所有历史文件，设置为-1(永不过期)，但不建议处理大量历史文件，配置remedy也不宜过大。

## Topic configuration properties

目前不支持重复topic，为防止重复填写，topic的名称需要写在配置文件段名中，例如：[bid-deal]，多次填写会被覆盖。

Property | type | Range | Default | Description
---|---|---|---|---
dirs | string | | | topic日志所在的目录，可以配置多个，使用','分割
remedy | int | -2147483647 - 2147483647 | Default configuration | 历史文件过期时间(s)，0不处理任何历史文件，小于0表示永不过期
batch.num | int | 1 - 2147483647 | Default configuration | produce每批次发送的message数量，发送后会更新offset信息
poll.timeout | int | 1 - 2147483647 | Default configuration | kafka client poll timeout，librdkafka poll函数，频繁调用
poll.messages | int | 1 - 2147483647 | Default configuration | kafka client队列满后，需要等待message减少的的数量

可以配置librdkafka topic configuration properties，需要在配置前上'kafka.'

例如：配置message.timeout.ms = 60000，需要写为kafka.message.timeout.ms = 60000

batch.num poll.timeout poll.messages 3个配置可以在运行时修改，命令：
```
kill -s SIGUSR1 $PID
```

# kafka2hdfs

## kafka configuration properties

see librdkafka configuration properties

注：老版kafka为0.8版本需要配置broker.version.fallback = 0.8.2(模板中已配好)

kafka.auto.offset.reset = smallest表明在offset文件不存在的情况下，会将offset设置为最小值，可以利用他实现补数(消费最老数据)，目前该项未配置或注释掉。

## hdfs configuration properties
Property | type | Range | Default | Description
---|---|---|---|---
type | string | command | | hdfs client 类型，目前仅支持command类型
namenode | string | | | namenode地址
port | int |  | | namenode端口
user | string | | | hdfs 用户
put | string | | hadoop fs -put | hdfs put命令
append | string | | hadoop fs -appendToFile | hdfs append命令
lzo.index | string | | hadoop jar /usr/hdp/2.4.0.0-169/hadoop/lib/hadoop-lzo-0.6.0.2.4.0.0-169.jar com.hadoop.compression.lzo.LzoIndexer | hdfs lzo索引命令

## Default configuration properties

default的配置会对所有topic生效，topic中可以覆盖default配置。

Property | type | Range | Default | Description
---|---|---|---|---
root.dir | string | | .(表示当前工作目录) | 消费的messages写入的本地文件，会自动在该目录下创建topic子目录
log.format | string |  | v6 | 具体信息见下方log.format
path.format | string | normal | normal | 格式化hdfs路径方式，目前仅支持normal
consume.type | string | | v6 | 具体信息见下方consume.type
upload.type | string |  | text | 具体信息见下方upload.type
parallel | int | 1-24 | 1 | 线程池数量，压缩和上传共用线程池，upload.type=text时，为防止多个进程append同一文件，强制为1
compress.lzo | string | | | lzo压缩命令，当upload.type=lzo时必须填写
compress.orc | string | | | orc压缩命令，当upload.type=orc时必须填写
compress.mv | string | | | 移动目录命令，已弃用
compress.appendcvt | string | | | appendcvt命令，当upload=appendcvt时必须填写
consume.interval | int |60-2147483647 | 900 | 文件归档时间间隔，单位秒，即900s内的数据会归档到同一文件内
complete.interval | int | 60-2147483647 | 120 | 文件完成的时间间隔，超过时间会停止写入，认为文件已写完，执行后续压缩和上传操作
complete.maxsize | long | | 21474836480 | 文件大小限制，超过大小会停止写入，小于等于0表示无限制
retention.seconds | int | -1-2147483647 | 0 | 文件的最大保留时间，超过会停止写入(根据atime判断),小于等于0表示无限制
upload.interval | int | 1-2147483647 | 20 | 压缩上传进程执行的时间间隔

可以配置librdkafka configuration properties，需要在配置前上'kafka.'

注:文件是否写完，执行后续压缩和上传由complete.interval，complete.maxsize和retention.seconds三个参数决定，超过任意一个都会停止写入。

## Topic configuration properties

partitions，offsets，hdfs.path和hdfs.path.delay是topic中的配置，其partitions，offsets，hdfs.path必须填写，hdfs.path.delay在upload.type=appendcvt时必须填写，其他配置如未设置会继承default中的配置，如配置会覆盖default中的配置。

目前不支持重复topic，为防止重复填写，topic的名称需要写在配置文件段名中，例如：[bid-deal]，多次填写会被覆盖。

Property | type | Range | Default | Description
---|---|---|---|---
partitions | int array | | | 消费的partitions，支持范围格式(‘1-2’)，支持','分隔
offsets | int array | -2，-1，-1000 | | partition对应的offset，partition之间使用','分隔，未填写的offset会拷贝配置的最后一个offset(-2：begginning；-1：end；-1000：stored)
hdfs.path | string | | | hdfs路径format，具体支持字段见hdfs.path
hdfs.path.delay | string | | | 当upload=appendcvt时必须填写
root.dir | string | | default property | 消费的messages写入的本地文件，会自动在该目录下创建topic子目录
log.format | string |  | default property | 具体信息见下方log.format
path.format | string | normal | default property | 格式化hdfs路径方式，目前仅支持normal
consume.type | string | | default property | 具体信息见下方consume.type
upload.type | string |  | default property | 具体信息见下方upload.type
parallel | int | 1-24 | default property | 线程池数量，压缩和上传共用线程池，upload.type=text时，为防止多个进程append同一文件，强制为1
compress.lzo | string | | default property | lzo压缩命令，当upload.type=lzo时必须填写
compress.orc | string | | default property | orc压缩命令，当upload.type=orc时必须填写
compress.mv | string | | default property | 移动目录命令，已弃用
compress.appendcvt | string | | default property | appendcvt命令，当upload=appendcvt时必须填写
consume.interval | int |60-2147483647 | default property | 文件归档时间间隔，单位秒，即900s内的数据会归档到同一文件内
complete.interval | int | 60-2147483647 | default property | 文件完成的时间间隔，超过时间会停止写入，认为文件已写完，执行后续压缩和上传操作
complete.maxsize | long | | default property | 文件大小限制，超过大小会停止写入，小于等于0表示无限制
retention.seconds | int | -1-2147483647 | default property | 文件的最大保留时间，超过会停止写入(根据atime判断),小于等于0表示无限制
upload.interval | int | 1-2147483647 | default property | 压缩上传进程执行的时间间隔

可以配置librdkafka configuration properties，需要在配置前上'kafka.'

hdfs.path hdfs.path.delay compress.lzo compress.orc compress.appendcvt consume.interval complete.interval complete.maxsize retention.seconds upload.interval可以在运行时修改：

修改配置文件后执行命令：
```
kill -s SIGUSR1 $PID
```

## hdfs.path

公共支持字段：

Section | Description
---|---
%Y | 年
%m | 月
%d | 日
%H | 时
%M | 分
%S | 秒
%t | topic
%T |　时间戳

扩展支持字段见log.format

## log.format

log format为增加抽取效率，仅抽取必要字段，如需增加抽取字段需要相应增加类型

Type | Log Format | Extract Log Field | Extend Field | Description
---|---|---|---|---
v6 | v6日志格式 | 1.10 requestTime | | impression impression_bid click click_bid使用
v6device | v6日志格式 | 1.10 requestTime 6.0 deviceType | %D device type(pc or mobile) | bid-deal bid-nodeal unbid-deal unbid-nodeal使用
ef | ef日志格式 | 6 ActionRequestTime | | 是用ef格式的topic较多，在此不列举，详细可见配置文件模板
efdevice | ef日志格式 | 6 ActionRequestTime 41 DeviceType | %D device type(pc or mobile) | bid unbid bid_aws unbid_aws使用
efic | ef日志格式，ic使用 | 2 ActionType 6 ActionRequestTime 41 DeviceType | %k(click imp_pc和imp_mobile) | ic使用，ic日志区分为click和impression，impression区分pc和mobile
eficaws| ef日志格式 icaws使用 | 2 ActionType 6 ActionRequestTime 41 DeviceType | %D device type(pc or mobile) %A action type(click or imp) | icaws使用，icaws日志区分为click和impression，impression区分pc和mobile，icaws将device和action分开
efimp | ef日志格式，imp使用 | 2 ActionType 6 ActionRequestTime | %A action type(click or imp) | imp使用
efstats | ef日志格式，stats使用 | 2 ActionType 6 ActionRequestTime | %A action type(click or imp) %p prefix(click or impression) | stats使用，需要根据action type匹配不同路径
pub | pub日志格式 | 11 RequestTime | | pub使用
report | 报表日志格式 | 0 RequestTime | | report使用
prebid | pre_bid_rec格式日志 | 1 RequestTime | | pre_bid_rec使用

## consume.type

Type | Description
---|---
v6 | kafka message写入文件
ef | 与v6相同，仅为便于区分
report | report类型会去掉日志的第一个时间字段
debug | 写入offset partition等调试信息，用于log2hdfs内部调试使用

## upload.type

Type | Description
---|---
text | text格式文件，为防止多线程追加同一文件，强制线程池线程数为1
lzo | lzo格式，对文件使用lzop压缩，上传后对文件创建索引
orc | orc格式，调用外部命令压缩为orc
compress | 移动到外部目录，外部程序压缩后移动回原目录，弃用
appendcvt | 对于延迟的cvt日志，除写入cvt日志外，还需要转化为固定格式，写入其他来源转化目录
textnoupload | 仅消费messages，不压缩，不上传hdfs
