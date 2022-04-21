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

package com.oppo.shuttle.rss.clients;

import com.oppo.shuttle.rss.common.ConfiguredServerList;
import com.oppo.shuttle.rss.common.Constants;
import com.oppo.shuttle.rss.common.ServerListDir;
import com.oppo.shuttle.rss.metadata.Ors2MasterServerManager;
import com.oppo.shuttle.rss.metadata.ZkShuffleServiceManager;
import com.oppo.shuttle.rss.server.master.ShuffleWorkerStatusManager;
import com.oppo.shuttle.rss.util.ShuffleUtils;
import org.apache.curator.test.TestingServer;
import org.apache.spark.testutil.Ors2MiniCluster;
import org.apache.spark.testutil.Ors2ShuffleMaster;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class Ors2ClientTest {

    private Ors2MiniCluster ors2MiniCluster = new Ors2MiniCluster(
            3,
            "localhost:2181",
            "master",
            Constants.TEST_DATACENTER_DEFAULT,
            Constants.TEST_CLUSTER_DEFAULT);

    private Ors2ShuffleMaster master01 = new Ors2ShuffleMaster(
            "localhost:2181",
            "com.oppo.ors2.server.master.RoundRobinDispatcher", 19189, 19188);

    private Ors2ShuffleMaster master02 = new Ors2ShuffleMaster(
            "localhost:2181",
            "com.oppo.ors2.server.master.RoundRobinDispatcher", 19179, 19178);

    private ZkShuffleServiceManager zkShuffleServiceManager;
    private Ors2MasterServerManager ors2MasterServerManager;

    private static TestingServer zkServer = null;


    @BeforeClass
    private void init() {
        try {
            zkServer = new TestingServer(2181);
            zkServer.start();
        } catch(Exception e) {
            throw new RuntimeException("start zk server failed");
        }
        master01.start();
        master02.start();
        ors2MiniCluster.start();
        zkShuffleServiceManager = new ZkShuffleServiceManager(
                "localhost:2181",
                5000,
                10);
        ors2MasterServerManager = new Ors2MasterServerManager(
                zkShuffleServiceManager,
                10000,
                1000,
                "default_master",
                false);
    }

    @AfterClass
    private void stop() throws IOException {
        ors2MiniCluster.shutdown();
        master02.stop();
        if(zkServer !=null){
            zkServer.stop();
        }
    }


    @Test
    public void getWorkersFromMaster() {
        ConfiguredServerList workers = ShuffleUtils.getShuffleServersWithoutCheck(zkShuffleServiceManager, 5,
                3 * 10000, Constants.TEST_DATACENTER_DEFAULT, Constants.TEST_CLUSTER_DEFAULT, "test1","aaa", 1, "", "tt1");
        ConfiguredServerList workers2 = ShuffleUtils.getShuffleServersWithoutCheck(ors2MasterServerManager, 2,
                3 * 10000, Constants.TEST_DATACENTER_DEFAULT, Constants.TEST_CLUSTER_DEFAULT, "test1", "bbb", 2, "", "tt2");
        Assert.assertEquals(workers.getServerDetailList().size(), 3);
        Assert.assertEquals(workers2.getServerDetailList().size(), 2);
    }


    @Test
    public void testFailOver() {
        ConfiguredServerList workers = ShuffleUtils.getShuffleServersWithoutCheck(zkShuffleServiceManager, 5,
                3 * 10000, Constants.TEST_DATACENTER_DEFAULT, Constants.TEST_CLUSTER_DEFAULT, "test1","aaa", 1, "", "tt1");
        Assert.assertEquals(workers.getServerDetailList().size(), 3);
        master01.stop();
        ConfiguredServerList workers2 = ShuffleUtils.getShuffleServersWithoutCheck(ors2MasterServerManager, 5,
                3 * 10000, Constants.TEST_DATACENTER_DEFAULT, Constants.TEST_CLUSTER_DEFAULT, "test1", "bbb", 1, "", "tt2");
        Assert.assertEquals(workers2.getServerDetailList().size(), 3);
    }

    @Test
    public void testUnregister() {
        ors2MiniCluster.shutdown();
        long totalCount = ShuffleWorkerStatusManager.getWorkerList()
                .values()
                .stream()
                .map(Map::values)
                .flatMap(Collection::stream)
                .map(ServerListDir::getHostStatusMap)
                .map(Map::values)
                .mapToLong(Collection::size)
                .sum();
        Assert.assertEquals(totalCount, 0);
    }
}
