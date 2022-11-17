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

import com.oppo.shuttle.rss.messages.ShuffleMessage;
import com.oppo.shuttle.rss.metadata.ZkShuffleServiceManager;
import com.oppo.shuttle.rss.server.master.ApplicationWhitelistController;
import com.oppo.shuttle.rss.server.master.ShuffleMaster;
import com.oppo.shuttle.rss.server.master.WeightedRandomDispatcher;
import com.oppo.shuttle.rss.util.ConfUtil;
import com.oppo.shuttle.rss.util.JsonUtils;
import io.netty.handler.codec.http.HttpUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.SetUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.test.TestingServer;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.Ors2ShuffleTestEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.testutil.Ors2ShuffleMaster;
import org.apache.zookeeper.CreateMode;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import scala.Int;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ApplicationWhitelistTest {
    static {
        System.setProperty(ConfUtil.RSS_CONF_DIR_PROP, "conf");
    }

    private ZkShuffleServiceManager zk;

    private void startZk() throws Exception {
        TestingServer zkServer = new TestingServer(2181, true);
        zkServer.start();
        zk = new ZkShuffleServiceManager("localhost:2181", 10000, 10000);


        ApplicationWhitelistController.WhitelistBean whitelist = new ApplicationWhitelistController.WhitelistBean();
        LinkedHashSet<String> taskSet = new LinkedHashSet<>();
        taskSet.add("dag1.t1");
        taskSet.add("dag1.t2");

        LinkedHashSet<String> nameSet = new LinkedHashSet<>();
        nameSet.add("test-name-pass");


        whitelist.setTaskIdList(taskSet);
        whitelist.setAppNameList(nameSet);
        System.out.println( JsonUtils.objToJson(whitelist));
        zk.createNode(zk.getWhitelistRoot(), JsonUtils.objToJson(whitelist).getBytes(), CreateMode.PERSISTENT);
    }

    @Test
    public void parseLog() throws IOException {
        List<String> lines = FileUtils.readLines(new File("D:\\Users\\Desktop\\1.log"));
        HashSet<Object> tasks = new HashSet<>();
        HashSet<Object> names = new HashSet<>();

        Pattern taskReg = Pattern.compile("taskId=(.*?),");
        Pattern appReg = Pattern.compile("appName=(.*?),");

        for (String l : lines) {
            Matcher matcher = taskReg.matcher(l);
            if (matcher.find()) {
                String taskId = matcher.group(1);
                if (!StringUtils.isEmpty(taskId)) {
                    tasks.add(taskId);
                } else {
                    Matcher matcher1 = appReg.matcher(l);
                    if (matcher1.find()) {
                        String appName = matcher1.group(1);
                        names.add(appName);
                    }
                }
            }
        }

        System.out.println(tasks);
        System.out.println(names);
    }

    @Test
    public void test1() throws Exception {
        startZk();
        ApplicationWhitelistController test = new ApplicationWhitelistController(zk,  true);

        ShuffleMessage.GetWorkersRequest.Builder dagBuilder = ShuffleMessage
                .GetWorkersRequest.newBuilder()
                .setDagId("dag1")
                .setAppName("app-name");

        assert test.checkIsWriteList(dagBuilder.setTaskId("t1").build());
        assert test.checkIsWriteList(dagBuilder.setTaskId("t2").build());
        assert !test.checkIsWriteList(dagBuilder.setTaskId("t3").build());

        ShuffleMessage.GetWorkersRequest.Builder nameBuilder = ShuffleMessage
                .GetWorkersRequest.newBuilder()
                .setAppId("app-1");
        //test appName
        assert test.checkIsWriteList(nameBuilder.setAppName("test-name-pass").build());
        assert test.checkIsWriteList(nameBuilder.setAppName("test-name-pass_12314124").build());
        assert !test.checkIsWriteList(nameBuilder.setAppName("test-name").build());
    }

    @Test
    public void runSpark() {
        Ors2ShuffleTestEnv env = Ors2ShuffleTestEnv.masterService();
        env.startCluster();
        SparkConf conf = new SparkConf()
                .set("spark.oflow.task.id", "t1");
        SparkSession spark = env.createSession(conf);

        long count = spark.range(0, 10000).count();
        System.out.println("count=" + count);
        env.stopCluster();
    }

    @Test
    public void httpTest() throws Exception {
        startZk();
        Ors2ShuffleMaster master = new Ors2ShuffleMaster(
                "localhost:2181",
                WeightedRandomDispatcher.class.getName(),
                1122,
                1123
        );

        master.start();

        // curl http://localhost:1123/whitelist
        // curl -XPOST http://localhost:1123/whitelist -d '{"dagIdList": ["dag1"], "taskIdList": ["dag2.http-t1", "dag2.http-t2"]}'
        // curl -XDELETE http://localhost:1123/whitelist -d '{"dagIdList": ["dag1"], "taskIdList": ["dag2.http-t1"]}'
        Thread.sleep(Int.MaxValue());
    }
}
