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

package com.oppo.shuttle.rss.testutil;

import com.oppo.shuttle.rss.ShuffleServerConfig;
import com.oppo.shuttle.rss.common.Constants;
import com.oppo.shuttle.rss.server.master.ShuffleMaster;
import com.oppo.shuttle.rss.metadata.ZkShuffleServiceManager;
import com.oppo.shuttle.rss.util.CommonUtils;
import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.function.Consumer;
import java.util.regex.Pattern;

public class TestShuffleMaster extends ShuffleMaster {
    private static final Logger logger = LoggerFactory.getLogger(TestShuffleMaster.class);
    private static TestingServer zkServer = null;
    private static final Pattern PORT_PATTERN = Pattern.compile("(?<=:)\\d+");

    public static void startServiceRegistryServer() {

        logger.info("Starting ZooKeeper test server");
        try {
            zkServer = new TestingServer(2181);
            zkServer.start();
        } catch(Exception e) {
            throw new RuntimeException("start zk server failed");
        }

    }

    public TestShuffleMaster(ShuffleServerConfig serverConfig, ZkShuffleServiceManager zkShuffleServiceManager) {
        super(serverConfig, zkShuffleServiceManager);
    }

    @Override
    public void shutdown() {
        super.shutdown(true);

        if (zkServer != null){
            try {
                zkServer.stop();
                zkServer = null;
            } catch (IOException e) {
                logger.error("Unable to shutdown ZooKeeper server", e);
            }
        }

        // use a socket to test and wait until server is closed
        CommonUtils.retry(10, 30000, () -> {
            try (Socket socket = new Socket()) {
                int timeout = 200;
                socket.connect(new InetSocketAddress("localhost", getMasterPort()), timeout);
                logger.info("Server still connectable on port {}", getMasterPort());
                return Constants.DEFAULT_SUCCESS;
            } catch (Throwable e) {
                logger.warn("Server not connectable on port {} which is expected due to server shutdown, exception: {}",
                        getMasterPort(), e.getMessage());
                return null;
            }
        });

    }

    public static TestShuffleMaster createRunningServer() {
        return createRunningServer(null);
    }

    public static TestShuffleMaster createRunningServer(Consumer<ShuffleServerConfig> configModifier) {
        // Creates with random ports.
        ShuffleServerConfig config = new ShuffleServerConfig();
        config.setMasterPort(0);
        config.setDataCenter(Constants.TEST_DATACENTER_DEFAULT);
        config.setCluster(Constants.TEST_CLUSTER_DEFAULT);

        if (configModifier != null) {
            configModifier.accept(config);
        }

        startServiceRegistryServer();

        ZkShuffleServiceManager zkManager =
                new ZkShuffleServiceManager(
                        "localhost:2181",
                        config.getNetworkTimeout(),
                        config.getNetworkRetries());
        return createRunningServer(config, zkManager);
    }

    public static TestShuffleMaster createRunningServer(ShuffleServerConfig serverConfig,
                                                        ZkShuffleServiceManager zkManager) {
        TestShuffleMaster server;

        try {
            server = new TestShuffleMaster(serverConfig, zkManager);
            server.run();
            logger.info(String.format("Started test shuffle master on port: %s", server.getMasterPort()));
            // use a client to test and wait until server is ready
//            RetryUtils.retryUntilTrue(10, TestConstants.NETWORK_TIMEOUT, () -> {
//                try (RegistryClient registryClient = new RegistryClient("localhost",
//                        server.getMasterPort(),
//                        TestConstants.NETWORK_TIMEOUT, "user1")) {
//                    registryClient.connect();
//                    return true;
//                } catch (Throwable ex) {
//                    return false;
//                }
//            });
            return server;
        } catch (Throwable e) {
            throw new RuntimeException("Failed to start stream server", e);
        }
    }
}
