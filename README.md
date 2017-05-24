## log2hdfs

log2hdfs is a c++ application to aggregator log files to hdfs.

## Usage

### Requirements

- The GNU toolchain
- GNU make
- cmake
- pthreads
- zlib (optional, for gzip compression support)
- libssl-dev (optional, for SSL and SASL SCRAM support)
- libsasl2-dev (optional, for SASL GSSAPI support)
- java (for kafka2hdfs hdfs client)
- hdfs (for kafka2jdfs hdfs client)

### Instructions

#### Building

##### Installing thirdparty 

```
cd thirdparty

sh download_thirdparty.sh
sh build_thirdparty.sh
```

Update versions.sh to change thirdparty version.

##### build log2kafka

```
sh build_log2kafka.sh

```

##### build kafka2hdfs

```
sh build_kafka2hdfs.sh
```

Need to configure HDFS_INCLUDE、HDFS_LIB and JAVA_LIB。

### Documentation

To generate Doxygen documents for the API, type:

```
doxygen Doxyfile
```

Configuration properties are documented in CONFIGURATION.md

Example configuration files are documented in conf/ sub-directory.

For a log2hdfs introduction, see INTRODUCTION.md

### Tests

See the test/ sub-directory.
