/*
 * Copyright 2021 OPPO. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oppo.shuttle.rss;

import com.oppo.shuttle.rss.common.Constants;
import com.oppo.shuttle.rss.messages.ShuffleMessage;
import com.oppo.shuttle.rss.server.master.WeightedRandomDispatcher;
import com.oppo.shuttle.rss.storage.ShuffleFileStorage;
import com.oppo.shuttle.rss.storage.ShuffleStorage;
import com.oppo.shuttle.rss.util.CommonUtils;
import org.apache.commons.cli.*;
import org.apache.parquet.Strings;

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

/**
 * Configuration for shuffle worker&master
 * @author oppo
 */
public class ShuffleServerConfig {

  // ***************** ShuffleMaster config *****************
  /**
   * heartBeat process thread count
   * process the heartbeat request from ShuffleWorker
   */
  private int heartBeatThreads = 5;

  private int masterPort = 19189;

  private int workerSelfCheckOkTimes = 3;
  private long workerPunishMills = 300000;
  private long workerCheckInterval = 15000;
  private long maxThroughputPerMin = 8 * 1024 * 1024 * 1024L;
  private long maxHoldDataSize = 20 * 1024 * 1024 * 1024L;
  private int maxFlowControlTimes = 10;
  public static final String DEFAULT_ROOT_DIR = "ors2-data";


  // ***************** ShuffleWorker config *****************
  private boolean useEpoll = true;

  private int shufflePort = 19190;

  private int buildConnectionPort = 19191;

  private int httpPort = 19188;

  private String rootDir = "";

  private int workerLoadWeight = 1;

  private int shuffleProcessThreads = 16;

  private int networkBacklog = 1000;

  private int networkTimeout = 30000;

  private int networkRetries = 5;

  private ShuffleStorage storage;

  private long appObjRetentionMillis = Constants.APP_OBJ_RETENTION_MILLIS_DEFAULT;

  private long appStorageRetentionMillis = Constants.APP_STORAGE_RETENTION_MILLIS_DEFAULT;

  private String dataCenter = Constants.TEST_DATACENTER_DEFAULT;
  private String cluster = Constants.TEST_CLUSTER_DEFAULT;
  private ShuffleMessage.ShuffleWorkerStorageType defaultStorageType = ShuffleMessage.ShuffleWorkerStorageType.SSD_HDFS;

  private String masterName = Constants.MASTER_NAME_DEFAULT;

  private String zooKeeperServers = "localhost:2181";

  private int zkConnBaseIntervalMs = 1000;
  private int zkConnMaxIntervalMs = 10000;

  private String dispatchStrategy = WeightedRandomDispatcher.class.getName();
  private String registryServer = null;

  public static final int DEFAULT_MAX_CONNECTIONS = 60000;
  private int maxConnections = DEFAULT_MAX_CONNECTIONS;

  public static final int DEFAULT_MAX_OPEN_FILES = 60000;
  private int maxOpenFiles = DEFAULT_MAX_OPEN_FILES;

  private long idleTimeoutMillis = Math.max(Constants.CLI_CONN_IDLE_TIMEOUT_MS + TimeUnit.MINUTES.toMillis(1),
          Constants.SERVER_CONNECTION_IDLE_TIMEOUT_MILLIS_DEFAULT);

  private long stateCommitIntervalMillis = 10000;

  private int shuffleWorkerDumpers = Runtime.getRuntime().availableProcessors();

  private int dumpersQueueSize = 100;

  private int retryBuildConnectBuffer = 30;
  private int priorityJobBuildConnectBuffer = 100;

  private boolean checkDataInShuffleWorker = false;

  private long updateDelay = 30 * 1000L;
  private long appControlInterval = 3600 * 1000L;
  private int numAppResourcePerInterval = 20;
  private int appNamePreLen = 25;
  private long blackListRefreshInterval = 300 * 1000L;
  private long clearShuffleDirInterval = 300 * 1000L;

  private String filterExcludes = "ors2,livy-session";

  public int getShuffleProcessThreads() {
    return shuffleProcessThreads;
  }

  public float getMemoryControlRatioThreshold() {
    return memoryControlRatioThreshold;
  }

  public void setMemoryControlRatioThreshold(float memoryControlRatioThreshold) {
    this.memoryControlRatioThreshold = memoryControlRatioThreshold;
  }

  public long getMemoryControlSizeThreshold() {
    return memoryControlSizeThreshold;
  }

  public void setMemoryControlSizeThreshold(long memoryContolSizeThreshold) {
    this.memoryControlSizeThreshold = memoryContolSizeThreshold;
  }

  public int getTotalConnections() {
    return totalConnections;
  }

  public void setTotalConnections(int totalConnections) {
    this.totalConnections = totalConnections;
  }

  public int getBaseConnections() {
    return baseConnections;
  }

  public void setBaseConnections(int baseConnections) {
    this.baseConnections = baseConnections;
  }

  // flow control config
  private float memoryControlRatioThreshold = 0.9F;
  private long memoryControlSizeThreshold = CommonUtils.getJvmConfigMaxMemory() / 2;
  private int totalConnections = 5100;
  private int baseConnections = 5000;
  private long connectionTimeoutInterval = 5 * 60 * 1000L;
  private long connectRetryInterval = 10 * 1000L;
  private long flowControlBuildIdTimeout = 60 * 1000L;

  public long getConnectionTimeoutInterval() {
    return connectionTimeoutInterval;
  }

  public long getConnectRetryInterval() {
    return connectRetryInterval;
  }

  public int getRetryBuildConnectBuffer() {
    return retryBuildConnectBuffer;
  }

  public void setRetryBuildConnectBuffer(int retryBuildConnectBuffer) {
    this.retryBuildConnectBuffer = retryBuildConnectBuffer;
  }

  public int getPriorityJobBuildConnectBuffer() {
    return priorityJobBuildConnectBuffer;
  }

  public void setPriorityJobBuildConnectBuffer(int priorityJobBuildConnectBuffer) {
    this.priorityJobBuildConnectBuffer = priorityJobBuildConnectBuffer;
  }

  public int getZkConnBaseIntervalMs() {
    return zkConnBaseIntervalMs;
  }

  public void setZkConnBaseIntervalMs(int zkConnBaseIntervalMs) {
    this.zkConnBaseIntervalMs = zkConnBaseIntervalMs;
  }

  public int getZkConnMaxIntervalMs() {
    return zkConnMaxIntervalMs;
  }

  public void setZkConnMaxIntervalMs(int zkConnMaxIntervalMs) {
    this.zkConnMaxIntervalMs = zkConnMaxIntervalMs;
  }

  public int getHeartBeatThreads() {
    return heartBeatThreads;
  }

  public void setHeartBeatThreads(int heartBeatThreads) {
    this.heartBeatThreads = heartBeatThreads;
  }

  public String getDispatchStrategy() {
    return dispatchStrategy;
  }

  public void setDispatchStrategy(String dispatchStrategy) {
    this.dispatchStrategy = dispatchStrategy;
  }

  public int getMasterPort() {
    return masterPort;
  }

  public void setMasterPort(int masterPort) {
    this.masterPort = masterPort;
  }

  public int getWorkerSelfCheckOkTimes() {
    return workerSelfCheckOkTimes;
  }

  public void setWorkerSelfCheckOkTimes(int workerSelfCheckOkTimes) {
    this.workerSelfCheckOkTimes = workerSelfCheckOkTimes;
  }

  public long getWorkerPunishMills() {
    return workerPunishMills;
  }

  public void setWorkerPunishMills(long workerPunishMills) {
    this.workerPunishMills = workerPunishMills;
  }

  public long getWorkerCheckInterval() {
    return workerCheckInterval;
  }

  public void setWorkerCheckInterval(long workerCheckInterval) {
    this.workerCheckInterval = workerCheckInterval;
  }

  public long getMaxThroughputPerMin() {
    return maxThroughputPerMin;
  }

  public void setMaxThroughputPerMin(long maxThroughputPerMin) {
    this.maxThroughputPerMin = maxThroughputPerMin;
  }

  public long getMaxHoldDataSize() {
    return maxHoldDataSize;
  }

  public void setMaxHoldDataSize(long maxHoldDataSize) {
    this.maxHoldDataSize = maxHoldDataSize;
  }

  public int getMaxFlowControlTimes() {
    return maxFlowControlTimes;
  }

  public void setMaxFlowControlTimes(int maxFlowControlTimes) {
    this.maxFlowControlTimes = maxFlowControlTimes;
  }

  public static ShuffleServerConfig buildFromArgs(String[] args) throws IOException {
    ShuffleServerConfig serverConfig = new ShuffleServerConfig();
    Options options = new Options();
    options.addOption("epoll", false, "Whether use epoll eventloop or not");
    options.addOption("port", true, "Shuffle port");
    options.addOption("buildConnectionPort", true, "Build connection port");
    options.addOption("baseConnections", true, "Flow control base connection num");
    options.addOption("totalConnections", true, "Flow control total connection num");
    options.addOption("memoryControlRatioThreshold", true, "Flow control memory control ratio threshold");
    options.addOption("memoryControlSizeThreshold", true, "Flow control memory control size threshold");
    options.addOption("httpPort", true, "Http server port");
    options.addOption("rootDir", true, "The base directory of writing shuffle files");
    options.addOption("nettyWorkerThreads", true, "Number of threads for netty to process socket data");
    options.addOption("heartBeatThreads", true, "Number of threads for netty to process worker heart beat");
    options.addOption("masterPort", true, "Shuffle master port");
    options.addOption("workerPunishMills", true, "Worker punish time in mills");
    options.addOption("workerCheckInterval", true, "Check worker status interval");
    options.addOption("maxThroughputPerMin", true, "Max throughput limit for shuffle worker");
    options.addOption("maxHoldDataSize", true, "Max data size limit for shuffle worker holding");
    options.addOption("maxFlowControlTimes", true, "Max flow control count for shuffle worker");
    options.addOption("networkBacklog", true, "Network back log size");
    options.addOption("networkTimeout", true, "Network connect timeout");
    options.addOption("networkRetries", true, "Network retry times");
    options.addOption("appObjRetentionMillis", true, "App memory retention mills");
    options.addOption("appStorageRetentionMillis", true, "App file retention millis");
    options.addOption("serviceRegistry", true, "Register shuffle worker type");
    options.addOption("dataCenter", true, "");
    options.addOption("cluster", true, "");
    options.addOption("zooKeeperServers", true, "zookeeper servers list");
    options.addOption("dispatchStrategy", true, "Master dispatch strategy for workers");
    options.addOption("zkConnBaseIntervalMs", true, "Connect zk server base sleep time ms");
    options.addOption("zkConnMaxIntervalMs", true, "Connect zk server max sleep time ms");
    options.addOption("registryServer", true,
            "Register server need to set when standalone mode is specified");
    options.addOption("dumperThreads", true, "Shuffle worker dump thread num");
    options.addOption("dumperQueueSize", true, "Shuffle worker dump queue size");
    options.addOption("checkDataInShuffleWorker", true,
            "Check coming data checksum and data size in shuffle worker");
    options.addOption("nettySendThreads", true, "Number of threads for netty to send msg to master");
    options.addOption("stateCommitIntervalMillis", true, "Shuffle worker send state info to master interval");
    options.addOption("shuffleWorkerStorageType", true, "Shuffle worker storage type");
    options.addOption("shuffleWorkerFileSystem", true, "Shuffle worker file system type");
    options.addOption("workerCheckInterval", true, "Worker health check interval");
    options.addOption("defaultStorageType", true, "Default storage type workers that distribute to driver");
    options.addOption("workerLoadWeight", true, "Shuffle worker distribute weight");
    options.addOption("masterName", true, "Shuffle master name");
    options.addOption("updateDelay", true, "Worker status update delay");
    options.addOption("appControlInterval", true, "Application control interval");
    options.addOption("numAppResourcePerInterval", true, "Apps run simultaneously each interval");
    options.addOption("blackListRefreshInterval", true, "Black refresh interval");
    options.addOption("filterExcludes", true, "App name exclude list");
    options.addOption("flowControlBuildIdTimeout", true, "build connection id timeout, default 60000ms");
    options.addOption("appNamePreLen", true, "App control pre length");
    options.addOption("maxOpenFiles", true, "Maximum number of open files");

    CommandLineParser parser = new BasicParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("java [OPTION]...", options);
      System.exit(1);
    }

    if (cmd.hasOption("epoll")) {
      serverConfig.useEpoll = true;
    }
    serverConfig.shufflePort = Integer.parseInt(cmd.getOptionValue("port", "19190"));
    serverConfig.buildConnectionPort =
            Integer.parseInt(cmd.getOptionValue("buildConnectionPort", "19191"));
    serverConfig.baseConnections = Integer.parseInt(cmd.getOptionValue("baseConnections", "5000"));
    serverConfig.totalConnections = Integer.parseInt(cmd.getOptionValue("totalConnections", "5100"));
    serverConfig.memoryControlRatioThreshold = Float.parseFloat(cmd.getOptionValue("memoryControlRatioThreshold",
            String.valueOf(serverConfig.memoryControlRatioThreshold)));
    serverConfig.memoryControlSizeThreshold = Long.parseLong(cmd.getOptionValue("memoryControlSizeThreshold",
            String.valueOf(serverConfig.memoryControlSizeThreshold)));
    serverConfig.httpPort = Integer.parseInt(cmd.getOptionValue("httpPort", "-1"));
    serverConfig.rootDir =
            cmd.getOptionValue("rootDir", Files.createTempDirectory("StreamServer_").toString());
    serverConfig.heartBeatThreads = Integer.parseInt(cmd.getOptionValue("heartBeatThreads", "5"));
    serverConfig.masterPort = Integer.parseInt(cmd.getOptionValue("masterPort", "19189"));
    serverConfig.workerPunishMills = Integer.parseInt(cmd.getOptionValue("workerPunishMills", "300000"));
    serverConfig.workerCheckInterval = Integer.parseInt(cmd.getOptionValue("workerCheckInterval", "15000"));
    serverConfig.maxThroughputPerMin = Long.parseLong(cmd.getOptionValue("maxThroughputPerMin", "4294967296"));
    serverConfig.maxHoldDataSize = Long.parseLong(cmd.getOptionValue("maxHoldDataSize", "21474836480"));
    serverConfig.maxFlowControlTimes = Integer.parseInt(cmd.getOptionValue("maxFlowControlTimes", "10"));
    serverConfig.shuffleProcessThreads = Integer.parseInt(cmd.getOptionValue("nettyWorkerThreads", "16"));
    serverConfig.networkBacklog = Integer.parseInt(cmd.getOptionValue("networkBacklog", "1000"));
    serverConfig.networkTimeout = Integer.parseInt(cmd.getOptionValue("networkTimeout", "30000"));
    serverConfig.networkRetries = Integer.parseInt(cmd.getOptionValue("networkRetries", "5"));
    serverConfig.appObjRetentionMillis = Long.parseLong(
            cmd.getOptionValue("appObjRetentionMillis",
                    String.valueOf(Constants.APP_OBJ_RETENTION_MILLIS_DEFAULT)));
    serverConfig.appStorageRetentionMillis = Long.parseLong(
            cmd.getOptionValue("appStorageRetentionMillis",
                    String.valueOf(Constants.APP_STORAGE_RETENTION_MILLIS_DEFAULT)));
    serverConfig.dataCenter = cmd.getOptionValue("dataCenter");
    serverConfig.dataCenter = cmd.getOptionValue("dataCenter", Constants.DATA_CENTER_DEFAULT);
    serverConfig.cluster = cmd.getOptionValue("cluster", Constants.CLUSTER_DEFAULT);
    serverConfig.masterName = cmd.getOptionValue("masterName", Constants.MASTER_NAME_DEFAULT);
    serverConfig.zooKeeperServers = cmd.getOptionValue("zooKeeperServers", "localhost:2181");
    serverConfig.dispatchStrategy = cmd.getOptionValue("dispatchStrategy", serverConfig.dispatchStrategy);
    serverConfig.zkConnBaseIntervalMs = Integer.parseInt(cmd.getOptionValue("zkConnBaseIntervalMs", "1000"));
    serverConfig.zkConnMaxIntervalMs = Integer.parseInt(cmd.getOptionValue("zkConnMaxIntervalMs", "10000"));
    serverConfig.registryServer = cmd.getOptionValue("registryServer");
    serverConfig.shuffleWorkerDumpers = Integer.parseInt(cmd.getOptionValue("dumperThreads",
            String.valueOf(serverConfig.shuffleWorkerDumpers)));
    serverConfig.dumpersQueueSize = Integer.parseInt(cmd.getOptionValue("dumperQueueSize",
            String.valueOf(serverConfig.dumpersQueueSize)));
    serverConfig.checkDataInShuffleWorker = Boolean.parseBoolean(cmd.getOptionValue("checkDataInShuffleWorker", "false"));
    serverConfig.stateCommitIntervalMillis = Long.parseLong(cmd.getOptionValue("stateCommitIntervalMillis", "10000"));
    serverConfig.defaultStorageType = ShuffleMessage.ShuffleWorkerStorageType.valueOf(cmd.getOptionValue("defaultStorageType", "SSD_HDFS"));
    serverConfig.workerLoadWeight = Integer.parseInt(cmd.getOptionValue("workerLoadWeight", "1"));
    serverConfig.updateDelay = Long.parseLong(cmd.getOptionValue("updateDelay", "30000"));
    serverConfig.appControlInterval = Long.parseLong(cmd.getOptionValue("appControlInterval", "3600000"));
    serverConfig.numAppResourcePerInterval = Integer.parseInt(cmd.getOptionValue("numAppResourcePerInterval", "20"));
    serverConfig.blackListRefreshInterval = Long.parseLong(cmd.getOptionValue("blackListRefreshInterval", "300000"));
    serverConfig.filterExcludes = cmd.getOptionValue("filterExcludes", "ors2,livy-session");
    serverConfig.flowControlBuildIdTimeout = Long.parseLong(cmd.getOptionValue("flowControlBuildIdTimeout", "60000"));
    serverConfig.appNamePreLen = Integer.parseInt(cmd.getOptionValue("appNamePreLen", "25"));
    serverConfig.maxOpenFiles  = Integer.parseInt(cmd.getOptionValue("maxOpenFiles", String.valueOf(serverConfig.maxOpenFiles)));

    serverConfig.storage = new ShuffleFileStorage(serverConfig.rootDir);
    return serverConfig;
  }

  public boolean isUseEpoll() {
    return useEpoll;
  }

  public void setUseEpoll(boolean useEpoll) {
    this.useEpoll = useEpoll;
  }

  public int getShufflePort() {
    return shufflePort;
  }

  public void setShufflePort(int port) {
    shufflePort = port;
  }

  public int getBuildConnectionPort() {
    return buildConnectionPort;
  }

  public void setBuildConnectionPort(int buildConnectionPort) {
    this.buildConnectionPort = buildConnectionPort;
  }

  public int getHttpPort() {
    return httpPort;
  }

  public void setHttpPort(int port) {
    httpPort = port;
  }

  public String getRootDirectory() {
    return rootDir;
  }

  public void setRootDirectory(String dir) {
    rootDir = dir;
  }

  public int getNetworkBacklog() {
    return networkBacklog;
  }

  public int getNetworkTimeout() {
    return networkTimeout;
  }

  public int getNetworkRetries() {
    return networkRetries;
  }

  public ShuffleStorage getStorage() {
    return storage;
  }

  public void setStorage(ShuffleStorage storage) {
    this.storage = storage;
  }

  public long getAppObjRetentionMillis() {
    return appObjRetentionMillis;
  }

  public void setAppObjRetentionMillis(long appObjRetentionMillis) {
    this.appObjRetentionMillis = appObjRetentionMillis;
  }

  public long getAppStorageRetentionMillis() {
    return appStorageRetentionMillis;
  }

  public void setAppStorageRetentionMillis(long appStorageRetentionMillis) {
    this.appStorageRetentionMillis = appStorageRetentionMillis;
  }

  public String getDataCenter() {
    return Strings.isNullOrEmpty(dataCenter) ? Constants.DATA_CENTER_DEFAULT : dataCenter;
  }

  public void setDataCenter(String dataCenter) {
    this.dataCenter = dataCenter;
  }

  public String getCluster() {
    return Strings.isNullOrEmpty(cluster) ? Constants.TEST_CLUSTER_DEFAULT : cluster;
  }

  public void setCluster(String cluster) {
    this.cluster = cluster;
  }

  public String getZooKeeperServers() {
    return zooKeeperServers;
  }

  public void setZooKeeperServers(String zooKeeperServers) {
    this.zooKeeperServers = zooKeeperServers;
  }

  public String getRegistryServer() {
    return registryServer;
  }

  public void setRegistryServer(String registryServer) {
    this.registryServer = registryServer;
  }

  public int getMaxConnections() {
    return maxConnections;
  }

  public void setMaxConnections(int maxConnections) {
    this.maxConnections = maxConnections;
  }

  public long getIdleTimeoutMillis() {
    return idleTimeoutMillis;
  }

  public void setIdleTimeoutMillis(long idleTimeoutMillis) {
    this.idleTimeoutMillis = idleTimeoutMillis;
  }

  public int getShuffleWorkerDumpers() {
    return shuffleWorkerDumpers;
  }

  public void setShuffleWorkerDumpers(int shuffleWorkerDumpers) {
    this.shuffleWorkerDumpers = shuffleWorkerDumpers;
  }

  public void setDumpersQueueSize(int dumpersQueueSize) {
    this.dumpersQueueSize = dumpersQueueSize;
  }

  public int getDumpersQueueSize() {
    return dumpersQueueSize;
  }

  public boolean isCheckDataInShuffleWorker() {
    return checkDataInShuffleWorker;
  }


  public long getStateCommitIntervalMillis() {
    return stateCommitIntervalMillis;
  }

  public int getAppNamePreLen() {
    return appNamePreLen;
  }

  public int getMaxOpenFiles() {
    return maxOpenFiles;
  }

  public void setMaxOpenFiles(int maxOpenFiles) {
    this.maxOpenFiles = maxOpenFiles;
  }

  public String getShuffleMasterConfig() {
    StringBuilder sb = new StringBuilder("ShuffleMasterConfig{useEpoll=").append(useEpoll)
            .append(", masterPort=").append(masterPort)
            .append(", heartBeatThreads=").append(heartBeatThreads)
            .append(", workerPunishMills=").append(workerPunishMills)
            .append(", workerSelfCheckOkTimes=").append(workerSelfCheckOkTimes)
            .append(", workerCheckInterval=").append(workerCheckInterval)
            .append(", maxFlowControlTimes=").append(maxFlowControlTimes)
            .append(", maxHoldDataSize=").append(maxHoldDataSize)
            .append(", maxThroughputPerMin=").append(maxThroughputPerMin)
            .append(", zooKeeperServers=").append(zooKeeperServers)
            .append(", networkTimeout=").append(networkTimeout)
            .append(", networkRetries=").append(networkRetries)
            .append(", dispatchStrategy=").append(dispatchStrategy)
            .append("}");
    return sb.toString();
  }

  public String getShuffleWorkerConfig() {
    StringBuilder sb = new StringBuilder("ShuffleWorkerConfig{useEpoll=").append(useEpoll)
            .append(", dataCenter=").append(dataCenter)
            .append(", cluster=").append(cluster)
            .append(", shufflePort=").append(shufflePort)
            .append(", buildConnectionPort=").append(buildConnectionPort)
            .append(", storage=").append(storage)
            .append(", shuffleProcessThreads=").append(shuffleProcessThreads)
            .append(", shuffleWorkerDumpers=").append(shuffleWorkerDumpers)
            .append(", dumpersQueueSize=").append(dumpersQueueSize)
            .append(", memoryControlRatioThreshold=").append(memoryControlRatioThreshold)
            .append(", memoryControlSizeThreshold=").append(memoryControlSizeThreshold)
            .append(", baseConnections=").append(baseConnections)
            .append(", maxConnections=").append(maxConnections)
            .append(", maxOpenFiles=").append(maxOpenFiles)
            .append(", rootDir=").append(rootDir)
            .append(", networkTimeout=").append(networkTimeout)
            .append(", networkRetries=").append(networkRetries)
            .append(", networkBacklog=").append(networkBacklog)
            .append(", zooKeeperServers=").append(zooKeeperServers)
            .append(", buildConnectionIdTimeout=").append(flowControlBuildIdTimeout)
            .append("}");
    return sb.toString();
  }

  public int getWorkerLoadWeight() {
    return workerLoadWeight;
  }

  public void setWorkerLoadWeight(int workerLoadWeight) {
    this.workerLoadWeight = workerLoadWeight;
  }

  public String getMasterName() {
    return masterName;
  }

  public long getUpdateDelay() {
    return updateDelay;
  }

  public long getAppControlInterval() {
    return appControlInterval;
  }

  public int getNumAppResourcePerInterval() {
    return numAppResourcePerInterval;
  }

  public long getBlackListRefreshInterval() {
    return blackListRefreshInterval;
  }

  public String getFilterExcludes() {
    return filterExcludes;
  }

  public long getClearShuffleDirInterval() {
    return clearShuffleDirInterval;
  }

  @Override
  public String toString() {
    return getShuffleMasterConfig()+" "+getShuffleWorkerConfig();
  }

  public long getFlowControlBuildIdTimeout() {
    return flowControlBuildIdTimeout;
  }

  public void setFlowControlBuildIdTimeout(long flowControlBuildIdTimeout) {
    this.flowControlBuildIdTimeout = flowControlBuildIdTimeout;
  }
}
