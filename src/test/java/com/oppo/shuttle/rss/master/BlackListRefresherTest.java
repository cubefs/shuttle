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

import com.oppo.shuttle.rss.server.master.BlackListRefresher;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

public class BlackListRefresherTest {


    @Test
    public void testBlackListConf() throws InterruptedException {
        BlackListRefresher blackListRefresher = new BlackListRefresher(0, 3000);
        Thread.sleep(3000);
        List<String> workerBlackList = blackListRefresher.getWorkerBlackList();
        Assert.assertEquals(workerBlackList.size(), 1);
    }
}
