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

import com.oppo.shuttle.rss.common.AppTaskInfo;
import com.oppo.shuttle.rss.common.Ors2ServerGroup;
import com.oppo.shuttle.rss.common.Ors2WorkerDetail;
import com.oppo.shuttle.rss.messages.ShuffleMessage;
import com.oppo.shuttle.rss.messages.ShufflePacket;
import com.google.protobuf.ByteString;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.Ors2Config;
import org.apache.spark.shuffle.Ors2ShuffleTestEnv;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class NettyClientTest {
    private Ors2ShuffleTestEnv env;

    @BeforeClass
    public void beforeClass() {
        env = Ors2ShuffleTestEnv.zkService();
        env.startCluster();
    }

    @AfterClass
    public void afterClass() {
        env.stopCluster();
    }


    @Test
    public void testBuild() throws Exception {
        ArrayList<Ors2WorkerDetail> details = new ArrayList<>();
        Ors2WorkerDetail workerDetail = env.getCluster().serverList().get(0);
        details.add(workerDetail);
        Ors2ServerGroup ors2ServerGroup = new Ors2ServerGroup(details);

        SparkConf conf = new SparkConf();
        conf.set(Ors2Config.networkTimeout(), 10 * 1000L);

        ArrayList<Ors2ServerGroup> groups = new ArrayList<>();
        groups.add(ors2ServerGroup);
        AppTaskInfo taskInfo = new AppTaskInfo("1", "0", 1, 1, 0, 1, 0, 0);
        NettyClient nettyClient = new NettyClient(groups, conf, taskInfo, new Ors2ClientFactory(conf));

        List<byte[]> bytes = Collections.singletonList(new byte[]{1, 1, 1});
        String id1 = NettyClient.requestId();
        String id2 = NettyClient.requestId();
        nettyClient.send(0, ShufflePacket.create(build(id1), createPackageRequest(id1), bytes));
        nettyClient.send(0, ShufflePacket.create(build(id2),  createPackageRequest(id2), bytes));
        nettyClient.waitFinish();

        Thread.sleep(5 * 1000);

        String id3 = NettyClient.requestId();
        String id4 = NettyClient.requestId();
        nettyClient.send(0, ShufflePacket.create(build(id3),  createPackageRequest(id3), bytes));
        nettyClient.send(0, ShufflePacket.create(build(id4),  createPackageRequest(id4), bytes));
        nettyClient.waitFinish();

        nettyClient.getClientFactory().stop();
    }

    public ShuffleMessage.BuildConnectionRequest.Builder build(String id) {
        return ShuffleMessage
                .BuildConnectionRequest.newBuilder().setVersion(1).setMessageId(id)
                .setJobPriority(1).setRetryIdx(0);
    }

    public ShuffleMessage.UploadPackageRequest.Builder createPackageRequest(String id) {
        byte[] data = new byte[] {1, 1, 1};
        ShuffleMessage.UploadPackageRequest.PartitionBlockData partitionBlock = ShuffleMessage
                .UploadPackageRequest
                .PartitionBlockData
                .newBuilder()
                .setPartitionId(1)
                .setDataLength(data.length)
                .setData(ByteString.copyFrom(data))
                .build();

        ShuffleMessage.UploadPackageRequest.Builder uploadPackageRequestBuilder = ShuffleMessage.UploadPackageRequest
                .newBuilder()
                .setAppId("app_test")
                .setAppAttempt("1")
                .setShuffleId(1)
                .setMapId(1)
                .setAttemptId(1)
                .setNumMaps(0)
                .setNumPartitions(1)
                .setMessageId(id)
                .setSeqId(1);

        return uploadPackageRequestBuilder;
    }
}
