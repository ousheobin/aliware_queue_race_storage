#!/bin/bash

LOG_HOME=/root
WORK_HOME=/alidata1/race2018
DATA_DIR=$WORK_HOME/data
CODE_DIR=$WORK_HOME/code

cd $WORK_HOME
rm -rf $CODE_DIR
rm -rf $DATA_DIR
mkdir $DATA_DIR

rm -rf ${LOG_HOME}/iostat.log
rm -rf ${LOG_HOME}/dstat.log
rm -rf ${LOG_HOME}/java.log

cd $WORK_HOME
git clone git@code.aliyun.com:ousheobin/aliware-queue-race.git /alidata1/race2018/code
cd $CODE_DIR
echo "Begin Build"
mvn clean package
echo "Begin IO Stat"
nohup iostat -x -m -d 1 > ${LOG_HOME}/iostat.log 2>&1 & echo $! > ${LOG_HOME}/iostat.pid
echo "Begin System Stat"
nohup dstat > ${LOG_HOME}/dstat.log 2>&1 & echo $! > ${LOG_HOME}/dstat.pid
echo "Begin Java Test"
java -Xms4g -Xmx4g -Dfile.encoding=UTF-8 -XX:+PrintGCDetails \
-classpath "$CLASSPATH:/alidata1/race2018/code/target/test-classes:/alidata1/race2018/code/target/Race2018/Race2018/lib/*" \
io.openmessaging.DemoTester > ${LOG_HOME}/java.log

echo "Test Finished..."

kill -9 $(cat ${LOG_HOME}/iostat.pid)
kill -9 $(cat ${LOG_HOME}/dstat.pid)
rm -rf ${LOG_HOME}/iostat.pid
rm -rf ${LOG_HOME}/dstat.pid