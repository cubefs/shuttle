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

import java.util.List;
import java.util.concurrent.*;

public class Ors2SimpleExecutorService extends Ors2AbstractExecutorService<Runnable> {
    private final Object[] locks;

    public Ors2SimpleExecutorService(int threadNum, int queueSize, int waitTimeout) {
       super(threadNum, queueSize, waitTimeout);

       locks = new Object[threadNum];
       for (int i = 0; i < threadNum; i++) {
           locks[i] = new Object();
       }
    }

    @Override
   public void execute(int value, Runnable command) {
        int index = getIndex(value);
        synchronized (locks[index]) {
            command.run();
        }
    }

    @Override
    public int size(int value) {
        return 0;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public List<Runnable> shutdownNow() {
        return null;
    }

    @Override
    public boolean isShutdown() {
        return false;
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }
}
