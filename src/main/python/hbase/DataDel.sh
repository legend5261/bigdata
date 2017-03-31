#!/bin/sh

HBASE_HOME=/home/hadoop/hbase

export PATH=${PATH}:${HBASE_HOME}/bin

################ start - python脚本运行列表################

echo '清理Hbase表数据开始...'
echo

echo '启动Hbase thrift server...'
hbase-daemon.sh start thrift
# 休眠3s，等待守护进程开启
sleep 3

python -u hbase_opration.py

echo
echo '数据清理结束'

echo '停止Hbase thrift server...'
hbase-daemon.sh stop thrift

#请不要把Python脚本放在此区块以外
################ end - python脚本运行列表################

exit
