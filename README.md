## LOG2HDFS
---

LOG2HDFS由LOG2KAFKA和KAFKA2HDFS两个部分组成。

实现文件从前端机发送到kafka，再从kafka消费上传到hdfs。

## Usage
---

### Requirements
---

- The GNU toolchain
- GNU make
- pthreads

## LOG2KAFKA
---

使用linux inotify递归监控目录，发送移动到目录中的文件到配置的kafka topic。

## KAFKA2HDFS
---

从kafka消费数据，按数据格式归档，上传到hdfs。
