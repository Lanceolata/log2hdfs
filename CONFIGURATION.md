# log2kafka

### kafka configuration properties

see librdkafka configuration properties

注：老版kafka为0.8版本需要配置broker.version.fallback = 0.8.2(模板中已配好)

### Global configuration properties

Property | type | Range | Default | Description
---|---|---|---|---
handle.dir | string | | remedy | 发送失败和超时的messages写入目录
handle.interval | int | 60 - 2147483647 | 1800 | errmsg_handle更新文件时间间隔，文件更新后，会根据handle.remedy配置决定是否重新发送
handle.remedy | bool | true，false | false | errmsg_handle是否重新发送到kafka
table.path | string | | offset_table | offset持久化文件
table.interval | 1 - 2147483647 | 30 | offset持久化到文件的时间间隔

### Default configuration properties

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

### Topic configuration properties

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

### hdfs configuration properties
Property | Range | Default | Description
---|---|---|---
type | command | command | hdfs client 类型
namenode | | | namenode地址
port | | | namenode端口
user | | | hdfs 用户
put | | | put命令
append | | | append命令
lzo.index | | | lzo索引命令

### Default configuration properties
Property | Range | Default | Description
---|---|---|---
log.format | v6 v6device ef efdevice | v6 | 日志类型，对应v6及ef日志
path.format | normal | normal | 路径格式类型
consume.type | report v6 ef debug | | consume callback类型:report类型会去吊日志的第一个时间字段，v6 ef为相同日志类型，debug会写入调试信息
file.format | orc lzo text compress | text | 文件格式：text文件文件，hdfs文件存在会追加；orc通过命令压缩为orc文件，hdfs文件存在会删除；lzo通过命令压缩为lzo文件，hdfs文件存在会删除，会生成index；compress移动给其他程序压缩，hdfs文件存在会删除。
parallel | 1-24 | | 线程池数量，text格式为防止多个进程append同一文件，强制为1；压缩和上传共用线程池
compress.lzo | | | lzo压缩命令
compress.orc | | | orc压缩命令
compress.mv | | | 移动目录
consume.interval | 60-2147483647 | 900 | 文件归档时间间隔
complete.interval | 60-2147483647 | 120 | 文件完成的时间间隔，超过时间会停止写入
complete.maxsize | 0-2147483647 | 21474836480 | 文件的最大大小，超过大小会停止写入
complete.maxseconds | -1-2147483647 | -1 | 文件的最大保留时间，超过会停止写入(根据atime判断)
upload.interval | 0-2147483647 | 20 | 上传文件扫描间隔

可以配置librdkafka configuration properties，需要在配置前上'kafka.'

### Topic configuration properties

Property | Range | Default | Description
---|---|---|---
topics | | | topics，可以配置多个，通过';'间隔
partitions | | | partitions，topic之间用';'间隔，支持范围格式(‘1-2’)
offsets | -2，-1，-1000 | | partition对应的offset，topic之间用';'间隔  -2：begginning；-1：end；-1000：stored；
hdfs.path | | | hdfs路径format，支持年(%Y) 月(%m) 日(%d) 时(%H) 分(%M) 秒(%S) section(%s) device(%D) type(%T) time stamp(%t)，及logformat的自定义类型(需要扩展实现)
log.format | v6 v6device ef efdevice | v6 | 日志类型，对应v6及ef日志
path.format | normal | normal | 路径格式类型
consume.type | report v6 ef debug | | consume callback类型:report类型会去吊日志的第一个时间字段，v6 ef为相同日志类型，debug会写入调试信息
file.format | orc lzo text compress | text | 文件格式：text文件文件，hdfs文件存在会追加；orc通过命令压缩为orc文件，hdfs文件存在会删除；lzo通过命令压缩为lzo文件，hdfs文件存在会删除，会生成index；compress移动给其他程序压缩，hdfs文件存在会删除。
parallel | 1-50 | | 线程池数量，text格式为防止多个进程append同一文件，强制为1；压缩和上传共用线程池
compress.lzo | | | lzo压缩命令
compress.orc | | | orc压缩命令
compress.mv | | | 移动目录
consume.interval | 60-2147483647 | 900 | 文件归档时间间隔
complete.interval | 60-2147483647 | 120 | 文件完成的时间间隔，超过时间会停止写入
complete.maxsize | 0-2147483647 | 21474836480 | 文件的最大大小，超过大小会停止写入
complete.maxseconds | -1-2147483647 | 0 | 文件的最大保留时间，超过会停止写入(根据atime判断) 小于等于0表示无限制
upload.interval | 0-2147483647 | 20 | 上传文件扫描间隔

可以配置librdkafka configuration properties，需要在配置前上'kafka.'

hdfs.path consume.interval complete.interval complete.maxsize complete.maxseconds upload.interval可以在运行时修改：
```
kill -s SIGUSR1 $PID
```

### kafka configuration properties

see librdkafka configuration properties
