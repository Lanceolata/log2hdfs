# 部署

1. 下载项目包
2. 解压，如果解压缩后目录不为log2hdfs，则改名为log2hdfs
3. cd log2hdfs/thirdparty
4. sh download_thirdparty.sh(下载第三方依赖库)
5. sh build_thirdparty.sh(构建第三方依赖库)
6. cd log2hdfs
7. sh build_kafka2hdfs.sh(编译kafka2hdfs)，编译后的执行程序kafka2hdfs会放到bin目录下

    注：kafka2hdfs使用了hdfs和jvm库，需要配置hdfs和jvm路径，目前新老集群已配置，如需要在其他系统中编译，需要修改脚本。

8. 在log2hdfs同级目录创建kafka2hdfs，并在kafka2hdfs下创建模块目录，例如kafka2hdfs/v6
9. 拷贝log2hdfs/bin/run_kafka2hdfs.sh到模块目录下(为保证更新代码过程中不会对正在运行的进程产生影响，需要在代码目录之外进行部署)

# 运行

## 运行

需要手动拷贝log2hdfs/conf/kafka2hdfs/下配置文件模板到模块下，根据需求修改配置文件。

    详细配置见模板和CONFIGURATION.md
    
    配置文件模板：
    1. v6.conf v6-log.conf处理impression impression_bid click click_bid view日志
    2. v6-bid.conf v6-bid-log.conf处理bid-deal bid-nodeal unbid-deal日志
    3. v6-unbid[0-4].conf v6-unbid[0-4]-log.conf处理unbid-nodeal日志，由于日志量较大，并需要压缩为orc格式，对io压力较大，分散到了5台机器上
    4. v6-aws.conf v6-aws-log.conf在新hadoop集群上处理老kafka集群adv cvt idm udcr bid_aws unbid_aws ic_aws日志
    5. report.conf report-log.conf处理报表日志
    6. report-optimusprime.conf report-optimusprime-log.conf处理报表日志，落地到小集群，为大集群的备份
    7. ef.conf ef-log.conf处理老集群日志，topic较多不列举，详见配置文件
    8. ef-cm.conf ef-cm-log.conf处理老集群cookie mapping日志，topic较多不列举，详见配置文件
    9. ef-self.conf ef-self-log.conf处理老集群self media日志，topic较多不列举，详见配置文件
    10. ef-bid.conf ef-bid-log.conf处理老集群bid日志
    11. ef-unbid[0-2].conf ef-unbid[0-2]-log.conf处理老集群unbid日志
    12. v6-sandbox.conf v6-sandbox-log.conf处理沙箱日志
    13. report-sandbox.conf report-sandbox-log.conf处理沙箱报表日志

运行；
```
cd /data/users/data-infra/kafka2hdfs/$moudle && sh run_kafka2hdfs.sh $type >> run_kafka2hdfs.log 2>&1
```
或加入到crontab中
```
* * * * * cd /data/users/data-infra/kafka2hdfs/$moudle && sh run_kafka2hdfs.sh $type >> run_kafka2hdfs.log 2>&1
```

注：
1. 运行沙箱环境$type需要设置为具体类型，例如sandbox环境$type为sandbox，与配置文件配套
2. 配置配套的两个配置文件，一个是topic相关的具体配置，一个是kafka2hdfs自身日志的配置，需要都拷贝到运行目录

## 增加/减少topic 修改配置

在配置文件中，增加/减少topic配置，或修改topic中支持运行时修改的配置

发送SIGUSR1信号到kafka2hdfs进程，kafka2hdfs会进行处理
```
kill -s SIGUSR1 $PID
```

# 停止

停止进程
```
kill $PID
```

程序内部会处理信号和线程并安全退出

完全停止进程需要较长时间(大约30秒)

# 机器 <--> 模块

机器与各模块的对应关系

IP | Module | Path
---|---|---
192.168.145.211 | ef | /data/users/data-infra/kafka2hdfs/ef
192.168.145.211 | ef-cm | /data/users/data-infra/kafka2hdfs/ef-cm
192.168.145.211 | ef-self | /data/users/data-infra/kafka2hdfs/ef-self
192.168.145.214 | ef-unbid0 | /data/users/data-infra/kafka2hdfs/ef-unbid0
192.168.145.214 | ef-unbid1 | /data1/users/data-infra/kafka2hdfs/ef-unbid1
192.168.145.215 | ef-unbid2 | /data1/users/data-infra/kafka2hdfs/ef-unbid2
192.168.145.215 | ef-bid | /data1/users/data-infra/kafka2hdfs/ef-bid
192.168.145.240 | report-optimusprime | /data/users/data-infra/kafka2hdfs/report-optimusprime
192.168.145.241 | report | /data/users/data-infra/kafka2hdfs/report
192.168.145.242 | v6 | /data/users/data-infra/kafka2hdfs/v6
192.168.145.242 | v6-aws | /data/users/data-infra/kafka2hdfs/v6-aws
192.168.145.242 | v6-bid | /data/users/data-infra/kafka2hdfs/v6-bid
192.168.145.243 | v6 | /data/users/data-infra/kafka2hdfs/v6 text格式文件备份
192.168.145.243 | v6-unbid0 | /data/users/data-infra/kafka2hdfs/v6-unbid0
192.168.145.244 | v6-bid | /data/users/data-infra/kafka2hdfs/v6-bid lzo格式文件备份
192.168.145.244 | v6-unbid1 | /data/users/data-infra/kafka2hdfs/v6-unbid1
192.168.145.245 | v6-unbid2 | /data/users/data-infra/kafka2hdfs/v6-unbid2
192.168.145.246 | v6-unbid3 | /data/users/data-infra/kafka2hdfs/v6-unbid3
192.168.145.247 | v6-unbid4 | /data/users/data-infra/kafka2hdfs/v6-unbid4
192.168.145.248 | | 备用机  目前空余
192.168.163.215 | v6-sandbox | /data/users/data-infra/kafka2hdfs/v6-sandbox
192.168.163.215 | report-sandbox | /data/users/data-infra/kafka2hdfs/report-sandbox

# topic <--> orc schema

## 基础日志

Topic | Schema 
---|---
impression | ic
impression_bid | ic-bid
click | ic
click_bid | ic-bid
bid-deal | bid
bid-nodeal | bid
unbid-deal | bid
unbid-nodeal | bid
bid_aws | bid-aws
unbid_aws | bid-aws

注：ic和bid为相同结构，仅为便于区分

## 报表

Topic | Schema
---|---
report.pv_account | report.base
report.base_reach_click | report.base
report.base_second_jump | report.base
report.base_conversion_click | report.base
report.base_conversion_imp | report.base
report.conversion_click | report.conversion_click
report.reach_click | report.reach_click
report.second_jump | report.second_jump
report.pdb_analysis | report.pdb_analysis
report.stats_service | report.stats_service
report.rpt_effect_pdb_return_reason | report.rpt_effect_pdb_return_reason
