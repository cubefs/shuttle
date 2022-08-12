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

package com.oppo.shuttle.rss.server.master;

import com.oppo.shuttle.rss.BuildVersion;
import com.oppo.shuttle.rss.ShuffleServerConfig;
import com.oppo.shuttle.rss.common.Constants;
import com.oppo.shuttle.rss.exceptions.Ors2Exception;
import com.oppo.shuttle.rss.metadata.ZkShuffleServiceManager;
import com.oppo.shuttle.rss.metrics.Ors2MetricsConstants;
import com.oppo.shuttle.rss.metrics.Ors2MetricsExport;
import com.oppo.shuttle.rss.server.ShuffleServer;
import com.oppo.shuttle.rss.util.NetworkUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.parquet.Strings;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * ShuffleMaster supplies three main functions:
 * 1. manage ShuffleWorker, add ShuffleWorker to or release it from blacklist
 * 2. dispatch shuffle workers for job requesting, using plugin dispatch strategy (default RoundRobin)
 * 3. route job to request different shuffle cluster resources, update shuffle service gracefully
 *
 * @author oppo
 */
public class ShuffleMaster extends ShuffleServer {
    private static final Logger logger = LoggerFactory.getLogger(ShuffleMaster.class);

    private final ShuffleServerConfig masterConfig;
    private final String localIp;
    private String serverId;
    private EventLoopGroup shuffleMasterEventLoopGroup;
    private EventLoopGroup httpEventLoopGroup;
    private ZkShuffleServiceManager zkManager;
    private int masterPort;
    private int httpPort;
    private static final AtomicBoolean isLeader = new AtomicBoolean(true);
    private static String masterWatchPath;
    private List<Channel> channels = new ArrayList<>(2);

    private ShuffleWorkerStatusManager shuffleWorkerStatusManager;
    private ShuffleMasterClusterManager shuffleMasterClusterManager;

    public ShuffleMaster(ShuffleServerConfig masterConfig) {
        this(masterConfig, null);
    }

    public ShuffleMaster(ShuffleServerConfig masterConfig, ZkShuffleServiceManager zkManager) {
        this.masterConfig = masterConfig;
        this.localIp = NetworkUtils.getLocalIp();
        this.zkManager = zkManager;
        init();
    }

    public int getMasterPort() {
        return masterPort;
    }

    @Override
    public String getServerId() {
        if (Strings.isNullOrEmpty(serverId)) {
            serverId = String.format("%s:%d", NetworkUtils.getLocalIp(), getMasterPort());
        }
        return serverId;
    }

    @Override
    protected void init() {
        this.masterPort = masterConfig.getMasterPort();
        this.httpPort = masterConfig.getHttpPort();
        this.serverId = getServerId();

        shuffleMasterEventLoopGroup = initEventLoopGroups(masterConfig.isUseEpoll(),
                masterConfig.getHeartBeatThreads(), "io-master-event");
        httpEventLoopGroup = initEventLoopGroups(masterConfig.isUseEpoll(),
                Constants.MASTER_HTTP_SERVER_THREADS, "io-master-http");

        if (zkManager == null) {
            this.zkManager = new ZkShuffleServiceManager(
                    masterConfig.getZooKeeperServers(),
                    masterConfig.getNetworkTimeout(),
                    masterConfig.getNetworkRetries());
        }
    }


    @Override
    protected void initChannel(String serverId, ChannelType type) throws InterruptedException {
        ApplicationWhitelistController applicationWhitelistController =
                new ApplicationWhitelistController(
                        zkManager,
                        masterConfig.isEnableWhiteListCheck()
                );

        switch (type) {
            case MASTER_HTTP_CHANNEL:
                String leaderAddr = createLeaderLatch();
                Supplier<ChannelHandler[]> httpSupplierHandlers = () -> new ChannelHandler[] {
                        new HttpServerCodec(),
                        new HttpObjectAggregator(512 * 1024),
                        new ShuffleMasterHttpHandler(isLeader, leaderAddr, applicationWhitelistController)
                };
                ServerBootstrap httpBootstrap = initServerBootstrap(
                        httpEventLoopGroup,
                        httpEventLoopGroup,
                        masterConfig,
                        httpSupplierHandlers);
                Channel httpChannel = bindPort(httpBootstrap, httpPort);
                channels.add(httpChannel);
                logger.info("Init master http channel finished, port: {}:{}.", localIp, httpPort);
                break;
            case MASTER_AGGREGATE_CHANNEL:
                ShuffleMasterDispatcher shuffleMasterDispatcher = getShuffleMasterDispatcher();
                // request controller
                ApplicationRequestController applicationRequestController =
                        new ApplicationRequestController(
                                masterConfig.getNumAppResourcePerInterval(),
                                masterConfig.getAppControlInterval(),
                                masterConfig.getUpdateDelay(),
                                masterConfig.getAppNamePreLen(),
                                masterConfig.getFilterExcludes());

                Supplier<ChannelHandler[]> masterSupplierHandlers = () -> new ChannelHandler[] {
                        new LengthFieldBasedFrameDecoder(134217728, 4, 4),
                        new ShuffleMasterHandler(
                                shuffleMasterDispatcher,
                                shuffleWorkerStatusManager,
                                applicationRequestController,
                                masterConfig.getMaxNumPartitions(),
                                applicationWhitelistController
                        )
                };

                ServerBootstrap masterBootstrap = initServerBootstrap(
                        shuffleMasterEventLoopGroup,
                        shuffleMasterEventLoopGroup,
                        masterConfig, masterSupplierHandlers);
                Channel masterChannel = bindPort(masterBootstrap, masterPort);
                channels.add(masterChannel);
                logger.info("Init master aggregate channel finished, port: {}:{}.", localIp, masterPort);
                break;
            default:
                logger.warn("InitChannel for shuffle master, invalid channel type: {}", type);
                break;
        }
    }

    public ShuffleMasterDispatcher getShuffleMasterDispatcher() {
        String shuffleMasterDispatcher = masterConfig.getDispatchStrategy();
        try {
            Class<?> clazz = Class.forName(masterConfig.getDispatchStrategy());
            Constructor<?> constructor = clazz.getConstructor(ShuffleServerConfig.class);
            return (ShuffleMasterDispatcher) constructor.newInstance(masterConfig);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new Ors2Exception(String.format("Failed to create ShuffleMasterDispatcher instance from class name %s", shuffleMasterDispatcher), e);
        }
    }

    @Override
    public void run() throws InterruptedException {
        logger.info("Starting shuffle master : {}", this.serverId);

        BlackListRefresher blackListRefresher =
                new BlackListRefresher(masterConfig.getUpdateDelay(), masterConfig.getBlackListRefreshInterval());
        shuffleWorkerStatusManager = new ShuffleWorkerStatusManager(masterConfig, blackListRefresher);
        shuffleWorkerStatusManager.updateStart();

        // watch dagId and storage type
        shuffleMasterClusterManager = new ShuffleMasterClusterManager(zkManager);

        masterWatchPath = this.zkManager.getMasterWatchPath(masterConfig.getMasterName());
        String dataCenter = masterConfig.getDataCenter();
        String cluster = masterConfig.getCluster();
        logger.info("Registering shuffle master, data center: {}, cluster: {}, server id: {}, ",
                dataCenter, cluster, serverId);

        this.zkManager.createNode(
                masterWatchPath,
                serverId.getBytes(StandardCharsets.UTF_8),
                CreateMode.PERSISTENT);
        logger.info("Create master watch path succeed : {}", masterWatchPath);

        initChannel(serverId, ChannelType.MASTER_AGGREGATE_CHANNEL);
        initChannel(serverId, ChannelType.MASTER_HTTP_CHANNEL);
    }

    private String createLeaderLatch() {
        LeaderLatch leaderLatch = this.zkManager.createLeaderLatcher(masterConfig.getMasterName(), serverId);
        String leaderHostAndPort = new String(zkManager.getZkNodeData(masterWatchPath), StandardCharsets.UTF_8);

        try {
            leaderLatch.start();
        } catch (Exception e) {
            throw new RuntimeException("start leaderLatch Exception: " + e.getCause());
        }
        isLeader.getAndSet(leaderLatch.hasLeadership());
        leaderLatch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                // set current server data to zk node
                logger.info("Current server {} is leader", serverId);
                for (int i = 0; i < 3; i++) {
                    try {
                        zkManager.setData(masterWatchPath, serverId.getBytes());
                        isLeader.getAndSet(true);
                        Ors2MetricsConstants.masterHaSwitch.inc();
                        return;
                    } catch (Exception e) {
                        logger.warn(e.getMessage());
                    }
                }
                // todo: shutdown current master?
                logger.error("shutdown master since it cannot work properly");
                ShuffleMaster.this.shutdown();
            }

            @Override
            public void notLeader() {
            }
        });
        return String.format("%s:%s", leaderHostAndPort.split(":")[0], masterConfig.getHttpPort());
    }

    @Override
    public void shutdown() {
        super.shutdown();
        shutdown(false);
    }

    public void shutdown(boolean wait) {
        try {
            zkManager.close();
        } catch (Throwable e) {
            logger.warn("Unable to shutdown metadata store:", e);
        }

        try {
            shuffleMasterClusterManager.close();
        } catch (IOException e) {
            logger.warn("Close shuffleWorkerClusterManager error, ", e);
        }

        closeChannels(channels);

        shutdownEventLoopGroup(shuffleMasterEventLoopGroup, "ShuffleMasterGroup");
        shutdownEventLoopGroup(httpEventLoopGroup, "ShuffleMasterHttpGroup");

        logger.info("ShuffleMaster shutdown finished: {}", serverId);
    }

    public static void main(String[] args) throws Exception {
        ShuffleServerConfig masterConfig = ShuffleServerConfig.buildFromArgs(args);
        logger.info("Starting master project version: {}, git commit version: {}) with config: {}",
                BuildVersion.projectVersion, BuildVersion.gitCommitVersion, masterConfig.getShuffleMasterConfig());
        ShuffleMaster master = new ShuffleMaster(masterConfig);

        try {
            master.run();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        addShutdownHook(master);

        Ors2MetricsExport.initialize(master.getServerId());
    }
}
