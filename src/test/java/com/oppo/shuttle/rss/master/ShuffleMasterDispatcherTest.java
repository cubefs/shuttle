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

package com.oppo.shuttle.rss.master;

import com.oppo.shuttle.rss.common.MasterDispatchServers;
import com.oppo.shuttle.rss.common.Ors2WorkerDetail;
import com.oppo.shuttle.rss.server.master.RoundRobinDispatcher;
import com.oppo.shuttle.rss.server.master.ShuffleMasterDispatcher;
import org.apache.curator.test.TestingServer;
import org.apache.spark.testutil.Ors2MiniCluster;
import org.apache.spark.testutil.Ors2ShuffleMaster;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.stream.Collectors;

public class ShuffleMasterDispatcherTest {

    private Ors2MiniCluster ssdHdfsCluster;
    private Ors2ShuffleMaster master;

    private String localZk = "localhost:2181";
    private static TestingServer zkServer = null;

    @BeforeClass
    public void setUp(){
        try {
            zkServer = new TestingServer(2181);
            zkServer.start();
        } catch(Exception e) {
            throw new RuntimeException("start zk server failed");
        }
        master = new Ors2ShuffleMaster(localZk, RoundRobinDispatcher.class.getName(), 19189, 19188);
        master.start();
        ssdHdfsCluster = new Ors2MiniCluster(10, localZk, "master", "SSD_HDFS", "cluster1");
        ssdHdfsCluster.start();

    }

    @AfterClass
    public void close() throws IOException {
        ssdHdfsCluster.shutdown();

        if (master != null){
            master.stop();
        }

        if(zkServer !=null){
            zkServer.stop();
        }
    }

    @Test
    public void testRoundRobin(){
        ShuffleMasterDispatcher dispatcher = master.getDispatcher();
        MasterDispatchServers availableServers1 =
                dispatcher.getServerList(3,"SSD_HDFS", "cluster1", "test11", 1);
        MasterDispatchServers availableServers2 =
                dispatcher.getServerList(3,"SSD_HDFS", "cluster1", "test11", 1);
        MasterDispatchServers availableServers3 =
                dispatcher.getServerList(3,"SSD_HDFS", "cluster1", "test11", 1);
        HashSet<String> allServers = new HashSet<>();
        allServers.addAll(availableServers1.getServerDetailList().stream().map(Ors2WorkerDetail::getServerId).collect(Collectors.toList()));
        allServers.addAll(availableServers2.getServerDetailList().stream().map(Ors2WorkerDetail::getServerId).collect(Collectors.toList()));
        allServers.addAll(availableServers3.getServerDetailList().stream().map(Ors2WorkerDetail::getServerId).collect(Collectors.toList()));
        Assert.assertEquals(allServers.size(), 9);
    }

}
