#!/bin/bash
HBASE_HOME=/home/hadoop/hbase
CLASSPATH=.:$HBASE_HOME/conf/hbase-site.xml

for i in ${HBASE_HOME}/lib/*.jar ;
do
    CLASSPATH=$CLASSPATH:$i
done
#编译程序
javac -cp $CLASSPATH HbaseJob.java
#执行程序
java -cp $CLASSPATH HbaseJob