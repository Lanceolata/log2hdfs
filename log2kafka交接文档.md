# 部署

使用脚本部署或拷贝部署，两种方式

### 使用部署脚本

在项目deploy目录下log2hdfs-deploy.py   使用fabric编写(需要安装fabric)

目前部署脚本只支持批量部署(dsp stats imp adp stats sandbox)，如需部署单独机器，需要修改脚本

详细部署过程见log2hdfs-deploy.py

部署配置见ipinyou_env.py

例如：

准备好安装包log2hdfs.tgz，开启python -m SimpleHTTPServer 6666  便于远程服务器下载安装(可以按具体情况修改脚本)。

部署沙箱环境

```
fab -f log2hdfs-deploy.py sandbox
```

部署脚本执行：
1. 登陆到远程服务器
2. 删除历史log2hdfs(删除时不会对正在运行的程序产生影响)
3. 解压缩log2hdfs.tgz
4. cd log2hdfs/thirdparty
5. sh download_thirdparty.sh(下载第三方依赖库)
6. sh build_thirdparty.sh(构建第三方依赖库)
7. cd log2hdfs
8. sh build_log2kafka.sh(构建log2kafka)，编译后的执行程序log2kafka会放到bin目录下
9. 在log2hdfs同级目录创建log2kafka
10. 拷贝log2hdfs/bin/run_log2kafka.sh 到log2kafka(为保证更新代码过程中不会对正在运行的进程产生影响，需要在代码目录之外进行部署)


### 手动安装

1. 拷贝安装包到安装目录
2. 解压缩log2hdfs.tgz
3. cd log2hdfs/thirdparty
4. sh download_thirdparty.sh(下载第三方依赖库)
5. sh build_thirdparty.sh(构建第三方依赖库)
6. cd log2hdfs
7. sh build_log2kafka.sh(构建log2kafka)，编译后的执行程序log2kafka会放到bin目录下
8. 在log2hdfs同级目录创建log2kafka
9. 拷贝log2hdfs/bin/run_log2kafka.sh 到log2kafka(为保证更新代码过程中不会对正在运行的进程产生影响，需要在代码目录之外进行部署)


# 运行

### 运行

需要手动拷贝log2hdfs/conf/log2kafka/下配置文件模板到log2kafka下，根据需求修改配置文件。

    详细配置见模板和CONFIGURATION.md
    
    配置文件模板：
    1. adp.conf adp-log.conf收集adp机器bid-deal和bid-nodeal日志
    2. adp-unbid.conf adp-unbid-log.conf收集adp机器unbid-deal和unbid-nodeal日志(由于unbid日志量较大，与bid分开可以见到对bid的影响)
    3. imp-v6.conf imp-v6-log.conf收集imp上新版impression和click日志
    4. dsp.conf dsp-log.conf收集老版dsp bid unbid pdb-bid pdb-unbid日志
    5. imp.conf imp-log.conf收集老版dsp imp unimp pdb-bid pdb-unbid日志
    6. dsp-aws.conf dsp-aws-log.conf收集欧洲及北美投放日志
    7. stats.conf stats-log.conf收集stats日志
    8. stats-cm.conf stats-log-cm.conf收集cookie mapping日志，由于一些cookie mapping日志一小时生成一个，为减小对其他stats日志的影响，所以分开
    9. stats-cmo.conf stats-cmo-log.conf 192.168.152.114/115/116上stats日志，与普通stats机器日志较为不同，所以分开
    10. sandbox.conf sandbox-log.conf收集沙箱环境日志

运行；
```
cd /data/users/data-infra/log2kafka && sh run_log2kafka.sh $type >> run_log2kafka.log 2>&1
```
或加入到crontab中
```
* * * * * cd /data/users/data-infra/log2kafka && sh run_log2kafka.sh $type >> run_log2kafka.log 2>&1
```

注：
1. 运行沙箱环境$type需要设置为具体类型，例如sandbox环境$type为sandbox，与配置文件配套，在运行脚本run_log2kafka.sh中写死，如需添加需要修改脚本
2. 配置配套的两个配置文件，一个是topic相关的具体配置，一个是log2kafka自身日志的配置，需要都拷贝到运行目录

### 增加/减少 topic

在配置文件中，增加/减少topic配置

发送SIGUSR1信号到log2kafka进程，log2kafla会进行处理
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

# 补数

在不修改程序的情况下：
1. 将需要发送的数据移动到临时目录
2. 单独配置log2kafka(topic及监控目录)，配置remedy参数为-1(负数表示发送目录中的所有数据)，注如存在相应的offset文件，需要删除
3. 修改run_log2kafka.sh脚本，目前脚本为防止运行错误，将可以运行的配置文件前缀写死在了脚本中，如增加配置文件前缀，需要修改run_log2kafka.sh中的array数组(或可以放开限制，将数据及匹配规则删除)
4. 运行log2kafka  会讲临时目录中的所有日志发送到kafka相应的topic

注：

如无必要，不要执行补数流程，大量的历史数据会对kafka和k2h程序造成影响