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
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.List;
import java.util.concurrent.*;

/**
 * User can specified the thread to process runnable task
 * Use Disruptor as thread runnable task queue
 */
public class Ors2SpecifiedExecutor extends Ors2AbstractExecutorService<Runnable> {
    private Disruptor<RunnableEvent>[] shuffleDataQueue;

    private volatile boolean closed = false;

    public Ors2SpecifiedExecutor(int threadNum, int queueSize, int waitTimeout) {
        super(threadNum, queueSize, waitTimeout);
        this.shuffleDataQueue = new Disruptor[this.threadNum];
        init();
    }

    private void init() {
        for (int i = 0; i < threadNum; i++) {
            final int threadIdx = i;
            Disruptor<RunnableEvent> disruptor = new Disruptor<RunnableEvent>(
                    RunnableEvent.RUNNABLE_EVENT_FACTORY, this.queueSize,
                    new ThreadFactory() {
                        @Override
                        public Thread newThread(Runnable runnable) {
                            return new Thread(threadName(threadIdx));
                        }
                    },
                    ProducerType.MULTI,
                    new BlockingWaitStrategy()
            );

            disruptor.handleEventsWith(new RunnableEventHandler());
            disruptor.setDefaultExceptionHandler(new DisruptorExceptionHandler());
            disruptor.start();
            shuffleDataQueue[i] = disruptor;
        }
    }

    @Override
    public void shutdown() {
        if (closed) {
            return;
        }

        for(Disruptor<RunnableEvent> queue : shuffleDataQueue) {
            queue.shutdown();
        }
        closed = true;
    }

    @Override
    public List<Runnable> shutdownNow() {
        return null;
    }

    @Override
    public boolean isShutdown() {
        return closed;
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public boolean awaitTermination(long l, TimeUnit timeUnit) throws InterruptedException {
        return false;
    }

    @Override
    public void execute(int value, Runnable command) {
        if (closed) {
            throw new Ors2Exception("The thread pool has been closed, cannot add tasks");
        }

        assert value < shuffleDataQueue.length : "execute thread index > shuffleDataQueue size";

        Disruptor<RunnableEvent> disruptor = shuffleDataQueue[value];
        if (disruptor != null) {
            RingBuffer<RunnableEvent> ringBuffer = disruptor.getRingBuffer();
            long seqIdx = -1;
            while (seqIdx == -1) {
                try {
                    seqIdx = ringBuffer.tryNext();
                } catch (InsufficientCapacityException e) {
                    logger.debug("RingBuffer has no space to publish");
                    try {
                        Thread.sleep(2L);
                    } catch (InterruptedException ie) {
                        logger.error("Waiting ringBuffer release space to publish, thread sleep interrupted!", e);
                    }
                }
            }

            if (seqIdx != -1) {
                RunnableEvent runnableEvent = ringBuffer.get(seqIdx);
                runnableEvent.setEvent(command);
                ringBuffer.publish(seqIdx);
            }
        } else {
            logger.error("Execute shuffleDataQueue disruptor is null");
            throw new Ors2Exception("Execute shuffleDataQueue disruptor is null");
        }
    }
}
