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

package com.oppo.shuttle.rss.execution;

import org.testng.annotations.Test;

import java.util.concurrent.ConcurrentLinkedDeque;

public class Ors2WorkerPartitionExecutorTest {

    @Test
    public void assignAndShutdown() throws Exception {
        int threadNum = 10;
        Ors2WorkerPartitionExecutor executor = new Ors2WorkerPartitionExecutor(threadNum);
        ConcurrentLinkedDeque<Integer> deque = new ConcurrentLinkedDeque<>();
        for (int i = 0; i < 100; i++) {
            final int d = i;
            executor.execute(i, () -> {
                System.out.println(Thread.currentThread().getName());
                deque.add(d);
                assert Thread.currentThread().equals(executor.getThread(d));
            });
        }

        executor.shutdown();
        assert deque.size() == 100;
    }

    @Test
    public void sameValue() {
        int threadNum = 10;
        Ors2WorkerPartitionExecutor executor = new Ors2WorkerPartitionExecutor(threadNum);

        int test = 1001;
        Thread executeThread = executor.getThread(test);
        for (int i = 0; i < 5; i++) {
            executor.execute(test, () -> {
                System.out.println(Thread.currentThread().getName());
                assert Thread.currentThread().equals(executeThread);
            });
        }

        executor.shutdown();
    }
}
