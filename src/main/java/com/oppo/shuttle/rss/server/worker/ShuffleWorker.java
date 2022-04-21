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

package com.oppo.shuttle.rss.server.worker;

import com.oppo.shuttle.rss.BuildVersion;
import com.oppo.shuttle.rss.ShuffleServerConfig;
import com.oppo.shuttle.rss.clients.Ors2RegistryClient;
import com.oppo.shuttle.rss.common.Ors2WorkerDetail;
import com.oppo.shuttle.rss.decoders.ShuffleWorkerMessageDecoder;
import com.oppo.shuttle.rss.exceptions.Ors2RegisterFailException;
import com.oppo.shuttle.rss.execution.BuildConnectionExecutor;
import com.oppo.shuttle.rss.execution.ShuffleDataExecutor;
import com.oppo.shuttle.rss.handlers.BuildConnectionInboundHandler;
import com.oppo.shuttle.rss.handlers.ShuffleDataIncomingHandler;
import com.oppo.shuttle.rss.messages.ShuffleMessage;
import com.oppo.shuttle.rss.messages.ShuffleWorkerStorageType;
import com.oppo.shuttle.rss.metadata.ServiceManager;
import com.oppo.shuttle.rss.metadata.ZkShuffleServiceManager;
import com.oppo.shuttle.rss.metrics.Ors2MetricsConstants;
import com.oppo.shuttle.rss.metrics.Ors2MetricsExport;
import com.oppo.shuttle.rss.server.ShuffleServer;
import com.oppo.shuttle.rss.util.NetworkUtils;
import com.oppo.shuttle.rss.util.ScheduledThreadPoolUtils;
import com.oppo.shuttle.rss.util.SelfCheckUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * ShuffleWorker supplies main function to group data for the same partition
 *
 * @author oppo
 */
public class ShuffleWorker extends ShuffleServer {
    private static final Logger logger = LoggerFactory.getLogger(ShuffleWorker.class);

    private ShuffleServerConfig serverConfig;
    private final String startVersion = String.valueOf(System.currentTimeMillis());
    private final String hostIp;
    private int shuffleDataPort;
    private int buildConnectionPort;

    private ZkShuffleServiceManager zkManager;

    /**
     * shuffle data server plugin
     */
    private ShuffleDataExecutor shuffleDataExecutor;
    private EventLoopGroup shuffleParentGroup;
    private EventLoopGroup shuffleChildGroup;

    /**
     * flow control server plugin
     */
    private EventLoopGroup buildConnBossGroup;
    private EventLoopGroup buildConnWorkerGroup;
    private BuildConnectionExecutor buildConnExecutor;

    private final List<Channel> channels = new ArrayList<>(2);

    /**
     * heartbeat client connect to shuffle master
     */
    private Ors2RegistryClient ors2RegistryClient;


    public ShuffleWorker(ShuffleServerConfig serverConfig) {
        this(serverConfig, null);
    }

    public ShuffleWorker(ShuffleServerConfig serverConfig, ZkShuffleServiceManager zkShuffleServiceManager) {
        this.hostIp = NetworkUtils.getLocalIp();
        this.serverConfig = serverConfig;
        init();
    }

    public void initWorkerClient() {
        try {
            this.ors2RegistryClient = new Ors2RegistryClient(
                    zkManager,
                    serverConfig.getConnectionTimeoutInterval(),
                    serverConfig.getConnectRetryInterval(),
                    serverConfig.getMasterName(),
                    serverConfig.isUseEpoll());

            sendHeartBeatToMaster();
        } catch (Ors2RegisterFailException e) {
            logger.error("Init worker client to master exception : ", e);
        }
    }

    private void sendHeartBeatToMaster() {
        long lastRestartTime = ManagementFactory.getRuntimeMXBean().getUptime();
        AtomicLong oldDataSize = new AtomicLong(0L);
        //send heartbeat and health info to master
        double rejectNumber = Ors2MetricsConstants.busyControlTimes.get() + Ors2MetricsConstants.memoryControlTimes.get();
        ScheduledThreadPoolUtils.scheduleAtFixedRate(() -> {
            logger.info("Send heartbeat to master");
            long holdDataSize = new Double(Ors2MetricsConstants.writeTotalBytes.get()).longValue();
            ors2RegistryClient.sendHeartbeat(ShuffleMessage.ShuffleWorkerHealthInfo.newBuilder()
                    .setHostIp(hostIp)
                    .setStartVersion(startVersion)
                    .setRootDirectory(serverConfig.getRootDirectory())
                    .setWorkerLoadWeight(serverConfig.getWorkerLoadWeight())
                    .setFsConf(serverConfig.getStorage().getConfASJson())
                    .setSelfCheckOK(SelfCheckUtils.selfCheck(serverConfig, hostIp))
                    .setLastRestartTime(lastRestartTime)
                    .setRejectConnNum((int) rejectNumber)
                    .setHoldDataSize(new Double(Ors2MetricsConstants.bufferedDataSize.get()).longValue())
                    .setThroughputPerMin(getThroughPut(holdDataSize, oldDataSize))
                    .setShufflePort(shuffleDataPort)
                    .setBuildConnPort(buildConnectionPort)
                    .setDataCenter(serverConfig.getDataCenter())
                    .setCluster(serverConfig.getCluster())
                    .setUpdateTime(System.currentTimeMillis())
                    .build());
            oldDataSize.set(holdDataSize);
        }, 0L, serverConfig.getStateCommitIntervalMillis());
    }

    private void unregisterFromMaster() {
        ors2RegistryClient.unregisterFromMaster(
                ShuffleMessage.ShuffleWorkerUnregisterRequest.newBuilder()
                        .setServerId(getServerId())
                        .build());
    }

    @Override
    protected void init() {
        shuffleDataPort = serverConfig.getShufflePort();
        buildConnectionPort = serverConfig.getBuildConnectionPort();
        this.shuffleParentGroup = initEventLoopGroups(serverConfig.isUseEpoll(), 1,"io-worker-boss");
        shuffleChildGroup = initEventLoopGroups(serverConfig.isUseEpoll(),
                serverConfig.getShuffleProcessThreads(), "io-worker-server");
        buildConnBossGroup = shuffleParentGroup;
        buildConnWorkerGroup = shuffleChildGroup;

        if (zkManager == null) {
            createZkManager(serverConfig);
        }

        this.shuffleDataExecutor = new ShuffleDataExecutor(serverConfig.getStorage(),
                serverConfig.getAppObjRetentionMillis(),
                serverConfig.getShuffleWorkerDumpers(),
                serverConfig.getDumpersQueueSize(),
                serverConfig.isCheckDataInShuffleWorker(),
                getWorkerId());

        this.buildConnExecutor = new BuildConnectionExecutor(serverConfig);
    }

    private void createZkManager(ShuffleServerConfig serverConfig) {
        this.zkManager = new ZkShuffleServiceManager(
                serverConfig.getZooKeeperServers(),
                serverConfig.getNetworkTimeout(),
                serverConfig.getZkConnBaseIntervalMs(),
                serverConfig.getNetworkRetries(),
                serverConfig.getZkConnMaxIntervalMs());
    }

    @Override
    protected void initChannel(String serverId, ChannelType type) throws InterruptedException {
        switch (type) {
            case WORKER_BUILD_CONN_CHANNEL:
                Supplier<ChannelHandler[]> buildSupplierHandlers = () -> new ChannelHandler[] {
                        new ShuffleWorkerMessageDecoder(),
                        new IdleStateHandler(0, 0, (int) serverConfig.getIdleTimeoutMillis() / 1000),
                        new BuildConnectionInboundHandler(serverId, startVersion, serverConfig.getIdleTimeoutMillis(), buildConnExecutor)
                };
                ServerBootstrap buildConnectionBootstrap = initServerBootstrap(
                        buildConnBossGroup, buildConnWorkerGroup, serverConfig, buildSupplierHandlers);
                Channel connChannel = bindPort(buildConnectionBootstrap, buildConnectionPort);
                channels.add(connChannel);
                logger.info("Init buildConnectionChannel ip: {} port:{}", hostIp, buildConnectionPort);
                break;
            case WORKER_DATA_CHANNEL:
                Supplier<ChannelHandler[]> dataSupplierHandlers = () -> new ChannelHandler[] {
                        new ShuffleWorkerMessageDecoder(),
                        new IdleStateHandler(0, 0, (int) serverConfig.getIdleTimeoutMillis() / 1000),
                        new ShuffleDataIncomingHandler(serverId, shuffleDataExecutor, buildConnExecutor)
                };
                ServerBootstrap dataServerBootstrap = initServerBootstrap(
                        shuffleParentGroup, shuffleChildGroup, serverConfig, dataSupplierHandlers);
                Channel shuffleChannel = bindPort(dataServerBootstrap, shuffleDataPort);
                channels.add(shuffleChannel);
                logger.info("Init shuffleDataChannel ip: {} port:{}", hostIp, shuffleDataPort);
                break;
            default:
                logger.warn("InitChannel for shuffle worker, invalid channel type: {}", type);
                break;
        }
    }

    @Override
    public void run() throws InterruptedException {
        String workerId = getWorkerId();
        String dc = serverConfig.getDataCenter();
        String cluster = serverConfig.getCluster();
        logger.info("Starting ShuffleWorker process, dataCenter: {}, cluster: {}, workerId: {}, rootDir: {}",
                dc, cluster, workerId, serverConfig.getRootDirectory());
        initChannel(workerId, ChannelType.WORKER_BUILD_CONN_CHANNEL);
        initChannel(workerId, ChannelType.WORKER_DATA_CHANNEL);
        String hostAndPort = Ors2WorkerDetail.createSimpleString(hostIp, shuffleDataPort, buildConnectionPort);
        logger.info("Registering shuffle server, data center: {}, cluster: {}, server id: {}, " +
                "host and port: {}", dc, cluster, workerId, hostAndPort);
        this.zkManager.registerServer(dc, cluster, hostAndPort, ServiceManager.ServerRole.SS_WORKER,
                ShuffleWorkerStorageType.SSD);
    }

    private long getThroughPut(long holdDataSize, AtomicLong oldDataSize) {
        return (holdDataSize - oldDataSize.longValue()) / (serverConfig.getStateCommitIntervalMillis() / 1000);
    }

    /**
     * Shuffle worker id: hostIP_dataPort
     *
     * @return
     */
    public String getWorkerId() {
        // use hostIp_shufflePort unify a shuffle worker
        return hostIp + "_" + serverConfig.getShufflePort();
    }

    @Override
    public String getServerId() {
        return getWorkerId();
    }

    @Override
    public void shutdown() {
        super.shutdown();
        shutdown(false);
    }

    public void shutdown(boolean wait) {
        try {
            if (ors2RegistryClient != null) {
                unregisterFromMaster();
            }
        } catch (Exception e) {
            logger.error("unregisterFromMaster exception:", e);
        }

        try {
            zkManager.close();
        } catch (Exception e) {
            logger.error("Shutdown zk manager exception:", e);
        }

        closeChannels(channels);
        shutdownEventLoopGroup(shuffleParentGroup, "ShuffleWorkerParentGroup");
        shutdownEventLoopGroup(shuffleChildGroup, "ShuffleWorkerChildGroup");

        try {
            shuffleDataExecutor.stop();
        } catch (Exception e) {
            logger.error("Unable to shutdown writer executor:", e);
        }

        try {
            if (ors2RegistryClient != null) {
                ors2RegistryClient.close();
            }
        } catch (Throwable ex) {
            logger.warn("Failed to close worker master client", ex);
        }
        logger.info("ShuffleWorker shutdown finished: {}", getServerId());
    }

    @Override
    public String toString() {
        return "ShuffleWorker{" +
                "workerId='" + getWorkerId() + '\'' +
                "startVersion='" + startVersion + '\'' +
                ", hostIp='" + hostIp + '\'' +
                ", shuffleDataPort=" + shuffleDataPort +
                '}';
    }

    public static void main(String[] args) throws Exception {
        ShuffleServerConfig serverConfig = ShuffleServerConfig.buildFromArgs(args);
        logger.info("Starting server project version: {}, git commit vision: {} with config: {}",
                BuildVersion.projectVersion, BuildVersion.gitCommitVersion, serverConfig.getShuffleWorkerConfig());
        ShuffleWorker shuffleWorker = new ShuffleWorker(serverConfig);

        try {
            shuffleWorker.run();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        shuffleWorker.initWorkerClient();
        addShutdownHook(shuffleWorker);
        Ors2MetricsExport.initialize(shuffleWorker.getWorkerId());
    }
}
