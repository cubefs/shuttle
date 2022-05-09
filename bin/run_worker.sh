#!/bin/bash

source "$(cd "`dirname "$0"`"; pwd)"/../conf/shuttle_env.sh
source "$(cd "`dirname "$0"`"; pwd)"/rss_common.sh

action=$1
export RSS_SERVER_NAME=worker_${RSS_DATA_CENTER}_${RSS_CLUSTER}

logDir=${RSS_HOME}/log-worker

pidFile=${RSS_HOME}/worker.pid

gracefullyTimeout=${RSS_GRACEFULLY_TIMEOUT}

mkdir -p ${logDir}

cd ${RSS_HOME}

check() {
  if [ -f "${pidFile}" ]; then
    local pid=`cat ${pidFile}`
    if kill -0 $pid > /dev/null 2>&1; then
      echo "Shuffle worker: ${RSS_SERVER_NAME} running, pid=${pid}. Please execute stop first"
      exit 1
    fi
  fi
}

waitPid() {
  pid=$1
  n=`expr $gracefullyTimeout / 3`
  i=0
  while [ $i -le $n ]; do
     if kill -0 $pid > /dev/null 2>&1; then
       echo "`date +"%Y-%m-%d %H:%M:%S"` wait shuffle worker: ${RSS_SERVER_NAME} stop gracefully"
       sleep 3
      else
        break
     fi
     let i++
  done
}

start() {
    check

    echo "" > ${logDir}/rss.out
    nohup ${JAVA_HOME}/bin/java \
    -server -XX:+UseG1GC -XX:G1HeapRegionSize=8m -verbose:GC -XX:+PrintGCDetails \
    -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps \
    -Xloggc:${logDir}/gc.log \
    -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=128M \
    -XX:+HeapDumpOnOutOfMemoryError \
    -XX:HeapDumpPath=${logDir}/rss_dump.hprof \
    -XX:+PrintStringTableStatistics \
    ${RSS_WORKER_MEMORY} -XX:MaxGCPauseMillis=300 -XX:InitiatingHeapOccupancyPercent=70 \
    -Dlog4j.configuration=file:${RSS_CONF_DIR}/log4j.properties -Dlog.dir=${logDir} \
    ${RSS_WORKER_JVM_OPTS} \
    -cp ${RSS_HOME}/lib/*.jar \
    com.oppo.shuttle.rss.server.worker.ShuffleWorker \
    ${RSS_WORKER_SERVER_OPTS} \
    > ${logDir}/rss.out 2>&1 < /dev/null  &

    newPid=$!
    sleep 3

    if [[ $(ps -p "$newPid" -o comm=) =~ "java" ]]; then
      echo $newPid > $pidFile
      # set_cgroup $newPid $RSS_CGROUP_CORES
      echo "shuffle worker: ${RSS_SERVER_NAME} start success, pid="$newPid
    else
      echo "shuffle worker: ${RSS_SERVER_NAME} start fail"
    fi

    head $logDir/rss.out
}

stop() {
  if [ -f "${pidFile}" ]; then
    local pid=`cat $pidFile`
    if kill -0 $pid > /dev/null 2>&1; then
      echo "stopping shuffle worker: ${RSS_SERVER_NAME}"

      kill $pid && rm -f "$pidFile"
      waitPid $pid;

      if kill -0 $pid > /dev/null 2>&1; then
        echo "shuffle worker: ${RSS_SERVER_NAME} did not stop gracefully after $gracefullyTimeout seconds: killing with kill -9"
        kill -9 $pid
      else
        echo "shuffle worker: ${RSS_SERVER_NAME} stop gracefully"
      fi
    else
      echo "no shuffle worker: ${RSS_SERVER_NAME} to stop"
    fi
  else
    echo "Not found shuffle worker: ${RSS_SERVER_NAME} pid file"
  fi
}

run() {
case $1 in
    "start")
        start
        ;;
    "stop")
        stop
        ;;
    "restart")
        stop
        sleep 1
        start
        ;;
     *)
        echo "Use age [start|stop|restart]"
        ;;
esac
}

run $action

