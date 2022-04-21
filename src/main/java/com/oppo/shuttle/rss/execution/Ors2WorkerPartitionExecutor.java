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

import com.oppo.shuttle.rss.exceptions.Ors2Exception;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class Ors2WorkerPartitionExecutor extends Ors2AbstractExecutorService<Runnable> {
    private final ArrayList<BlockingQueue<Runnable>> shuffleDataQueue;

    private final int POLL_TIMEOUT = 300;


    public Ors2WorkerPartitionExecutor(int threadNum, int queueSize, int waitTimeout) {
        super(threadNum, queueSize, waitTimeout);
        this.shuffleDataQueue = new ArrayList<>(threadNum);
        init();
    }

    public Ors2WorkerPartitionExecutor(int threadNum) {
        this(threadNum, 1024, 300);
    }

    private void init() {
        for (int i = 0; i < threadNum; i++) {
            BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(queueSize);

            Thread thread = new Thread(threadName(i)) {
                @Override
                public void run() {
                    Runnable task;
                    while (!isStop()) {
                        try {
                            while ((task = queue.poll(POLL_TIMEOUT, TimeUnit.MILLISECONDS)) != null) {
                                task.run();
                            }
                        } catch (Throwable e) {
                            logger.error("run fail", e);
                        }
                    }
                }
            };

            thread.setDaemon(true);
            thread.start();

            threads.add(i, thread);
            shuffleDataQueue.add(i, queue);
        }
    }

    @Override
    public synchronized void shutdown() {
        if (isShutdown()) {
            logger.warn("The thread pool has been shutdown");
            return;
        }

        advanceRunState(SHUTDOWN);

        while (true) {
            try {
                Thread.sleep(1000);
                logger.info("Wait for the task execution in the queue to complete");
            } catch (InterruptedException e) {
                logger.warn("queue wait", e);
            }

            int empty = 0;
            for(BlockingQueue<Runnable> queue : shuffleDataQueue) {
                if (queue.isEmpty()) {
                    empty += 1;
                }
            }

            if (empty == shuffleDataQueue.size()) {
                break;
            }
        }

        advanceRunState(STOP);

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                logger.warn("thread.join", e);
            }
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        throw new Ors2Exception("not support");
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        throw new Ors2Exception("not support");
    }

    @Override
    public void execute(int value, Runnable command) {
        if (isShutdown()) {
            throw new Ors2Exception("The thread pool has been shutdown, cannot add tasks");
        }

        try {
            boolean bool;
            if (waitTimeout <= 0) {
                getQueue(value).put(command);
                bool = true;
            } else {
                bool = getQueue(value).offer(command, waitTimeout, TimeUnit.MILLISECONDS);
            }

            if (!bool) {
                String name = getThread(value).getName();
                String msg = String.format("thread %s queue is full(queueSize=%s)", name, queueSize);
                throw new Ors2Exception(msg);
            }
        } catch (InterruptedException e) {
            throw new Ors2Exception(e);
        }
    }

    public BlockingQueue<Runnable> getQueue(int value) {
        return shuffleDataQueue.get(getIndex(value));
    }

    @Override
    public int size(int value) {
        return getQueue(value).size();
    }
}
