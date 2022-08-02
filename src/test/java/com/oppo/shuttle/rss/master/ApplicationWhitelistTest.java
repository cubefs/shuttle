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
import com.oppo.shuttle.rss.server.master.ApplicationWhitelistController;
import com.oppo.shuttle.rss.util.ConfUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.Ors2ShuffleTestEnv;
import org.apache.spark.sql.SparkSession;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ApplicationWhitelistTest {
    static {
        System.setProperty(ConfUtil.RSS_CONF_DIR_PROP, "conf");
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
    public void test1() {
        ApplicationWhitelistController test = new ApplicationWhitelistController( true);

        ShuffleMessage.GetWorkersRequest.Builder dagBuilder = ShuffleMessage
                .GetWorkersRequest.newBuilder()
                .setDagId("dag-test");

        assert !test.checkIsWriteList(dagBuilder.setDagId("other").build());
        assert test.checkIsWriteList(dagBuilder.setTaskId("task-test").build());
        assert test.checkIsWriteList(dagBuilder.setAppId("other").build());


        ShuffleMessage.GetWorkersRequest.Builder taskBuilder = ShuffleMessage
                .GetWorkersRequest.newBuilder()
                .setAppId("app-1")
                .setDagId("dag-1");

        // test dag id
        assert test.checkIsWriteList(taskBuilder.setTaskId("task-test").build());
        assert !test.checkIsWriteList(taskBuilder.setTaskId("other").build());

        ShuffleMessage.GetWorkersRequest.Builder nameBuilder = ShuffleMessage
                .GetWorkersRequest.newBuilder()
                .setAppId("app-1");
        //test appName
        assert test.checkIsWriteList(nameBuilder.setAppName("app-test_1231412").build());
        assert test.checkIsWriteList(nameBuilder.setAppName("App-test_abacdf").build());
        assert !test.checkIsWriteList(nameBuilder.setAppName("other").build());
    }

    @Test
    public void runSpark() {
        Ors2ShuffleTestEnv env = Ors2ShuffleTestEnv.masterService();
        env.startCluster();
        SparkConf conf = new SparkConf()
                .set("spark.oflow.task.id", "task-test");
        SparkSession spark = env.createSession(conf);

        long count = spark.range(0, 10000).count();
        System.out.println("count=" + count);
        env.stopCluster();
    }

    @Test
    public void test2() throws Exception {
        ApplicationWhitelistController test = new ApplicationWhitelistController( true);

        ShuffleMessage.GetWorkersRequest.Builder builder = ShuffleMessage
                .GetWorkersRequest.newBuilder()
                .setTaskId("task-test");
        assert test.checkIsWriteList(builder.build());

        assert !test.checkIsWriteList(builder.setTaskId("t2").build());
        Thread.sleep(TimeUnit.MINUTES.toMillis(1) + 1);
        // chang file, add t2
        assert test.checkIsWriteList(builder.setTaskId("t2").build());
    }
}
