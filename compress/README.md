## Compile

mvn clean compile package

打包到target目录下，compress-1.0-SNAPSHOT.jar

## Command

java -cp compress-1.0-SNAPSHOT.jar com.ipinyou.compress.OrcCompress -c conf_file -t topic file

-c  指定配置文件

-t  指定topic，该topic为配置文件中的topic，与实际topic不一定相符

file    指定压缩的文件，压缩后生成file.orc文件，源文件删除或移动到备份目录


