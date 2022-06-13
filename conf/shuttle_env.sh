#!/bin/bash


# Define the environment variables needed for the operation of RSS master and worker.
export JAVA_HOME=/usr/local/jdk8/
export RSS_HOME="$(cd "$(dirname "$0")"/..; pwd)"
export RSS_CONF_DIR=${RSS_HOME}/conf
export HADOOP_USER_NAME=hive

# Please set the hadoop local library directory here
export RSS_JAVA_LIB=/xxx

RSS_CLASSPATH=$(echo $RSS_HOME/lib/*.jar | tr ' ' ':')
export CLASSPATH=${RSS_CLASSPATH}

# Define the cluster
export RSS_DATA_CENTER=dc1
export RSS_CLUSTER=hdfs_g1
export RSS_ROOT_DIR=hdfs://test-hdfs/user/hive/rss-data

# Define the registration type.
export RSS_REGISTER_TYPE=master
export RSS_MASTER_NAME=master_g1
export RSS_ZK_SERVERS=localhost:2181

# Kill exit waiting time. If this time is exceeded, kill -9 will be executed
export RSS_GRACEFULLY_TIMEOUT=120

export RSS_MASTER_JVM_OPTS="-Dmetrics.report.psa=$RSS_PSA -Djava.library.path=$RSS_JAVA_LIB"
export RSS_WORKER_JVM_OPTS="$RSS_MASTER_OPTS"

export RSS_MASTER_MEMORY="-Xms2g -Xmx2g"
export RSS_WORKER_MEMORY="-Xms8g -Xmx8g"

# Limit the number of cpu cores used. Unlimited by default
export RSS_CGROUP_CORES=-1

register="-serviceRegistry ${RSS_REGISTER_TYPE} -masterName ${RSS_MASTER_NAME} -zooKeeperServers ${RSS_ZK_SERVERS} -dataCenter ${RSS_DATA_CENTER} -cluster ${RSS_CLUSTER}"

export RSS_MASTER_SERVER_OPTS="${register} -masterPort 19189 -httpPort 19188"

export RSS_WORKER_SERVER_OPTS="${register} -workerLoadWeight 1 -rootDir ${RSS_ROOT_DIR} -buildConnectionPort 19191 -port 19190"