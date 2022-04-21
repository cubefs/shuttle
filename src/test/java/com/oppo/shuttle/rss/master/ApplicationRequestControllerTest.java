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

import com.oppo.shuttle.rss.server.master.ApplicationRequestController;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ApplicationRequestControllerTest {

    @Test
    public void testOneAppRequest() throws ExecutionException, InterruptedException {
        ApplicationRequestController applicationRequestController =
                new ApplicationRequestController(3, 10000L, 5000, 10,"");
        applicationRequestController.updateStart();
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        for (int i = 0; i < 5; i++) {
            Future<Object> result =
                    executorService.submit(() -> applicationRequestController.requestCome("TEST1", "application_166859_2765"));
            Assert.assertTrue((Boolean) result.get());
        }
    }

    @Test
    public void testMultiAppRequest() {
        ApplicationRequestController applicationRequestController =
                new ApplicationRequestController(2, 3000L, 5000, 4, "");
        applicationRequestController.updateStart();
        boolean result1 = applicationRequestController.requestCome("TEST1", "application_16359_275");
        Assert.assertTrue(result1);
        boolean result2 = applicationRequestController.requestCome("TEST2", "application_16359_276");
        Assert.assertTrue(result2);
        boolean result3 = applicationRequestController.requestCome("TEST3", "application_16359_277");
        Assert.assertFalse(result3);
    }

    @Test
    public void testControllerRefresh() throws InterruptedException {
        ApplicationRequestController applicationRequestController =
                new ApplicationRequestController(2, 1000L, 2000, 4, "");
        applicationRequestController.updateStart();
        boolean result1 = applicationRequestController.requestCome("TEST1", "application_164359_275");
        Assert.assertTrue(result1);
        boolean result2 = applicationRequestController.requestCome("TEST2", "application_164359_276");
        Assert.assertTrue(result2);
        Thread.sleep(5000);
        boolean result3 = applicationRequestController.requestCome("TEST3", "application_164359_277");
        Assert.assertTrue(result3);
    }

}
