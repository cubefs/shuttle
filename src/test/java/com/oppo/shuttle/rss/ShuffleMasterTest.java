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

import com.oppo.shuttle.rss.common.ServerListDir;
import com.oppo.shuttle.rss.server.master.ShuffleDataDirClear;
import com.oppo.shuttle.rss.storage.ShuffleFileStorage;
import com.oppo.shuttle.rss.storage.fs.FileStatus;
import com.oppo.shuttle.rss.testutil.TestShuffleMaster;
import org.testng.annotations.Test;

import java.util.List;

public class ShuffleMasterTest {
    @Test
    public void startAndShutdown() {
        TestShuffleMaster runningServer = TestShuffleMaster.createRunningServer();
        runningServer.shutdown();
    }

    @Test
    public void clearShuffleDirTest() throws InterruptedException {
        ShuffleDataDirClear shuffleDataDirClear = new ShuffleDataDirClear(10000);
        ShuffleFileStorage storage = new ShuffleFileStorage("ors2-data");
        storage.createDirectories("ors2-data/clear/a");
        storage.createDirectories("ors2-data/clear/b");
        storage.createDirectories("ors2-data/clear/c");

        Thread.sleep(9000);
        shuffleDataDirClear.execute(new ServerListDir("ors2-data/clear", null));
        List<FileStatus> list = storage.listStatus("ors2-data/clear");
        assert (list.size() == 3);

        Thread.sleep(1000);
        shuffleDataDirClear.execute(new ServerListDir("ors2-data/clear", null));
        list = storage.listStatus("ors2-data/clear");
        assert (list.size() == 0);
    }
}
