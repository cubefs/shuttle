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

import com.oppo.shuttle.rss.clients.handler.Request;
import com.oppo.shuttle.rss.clients.handler.ResponseCallback;
import com.oppo.shuttle.rss.common.*;
import com.oppo.shuttle.rss.exceptions.Ors2NetworkException;
import com.oppo.shuttle.rss.messages.ShufflePacket;
import org.apache.commons.lang3.RandomUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.Ors2Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * netty nio client
 */
public class NettyClient {
    private static final Logger logger = LoggerFactory.getLogger(NettyClient.class);
    private final Ors2ServerSwitchGroup serverGroup;
    private final AppTaskInfo taskInfo;

    private final boolean mapWriteDispersion;

    private final AtomicInteger readySend = new AtomicInteger(0);
    private final AtomicInteger sendFinish = new AtomicInteger(0);

    private final Semaphore semaphore;

    private final int ioMaxRetry;
    private final long retryBaseWaitTime;

    private final ResponseCallback callback;

    private final Ors2ClientFactory clientFactory;

    public NettyClient(List<Ors2ServerGroup> groupList, SparkConf conf, AppTaskInfo taskInfo, Ors2ClientFactory clientFactory) {
        long netWorkTimeout = (long) conf.get(Ors2Config.networkTimeout());
        ioMaxRetry = Math.max((int) conf.get(Ors2Config.sendDataMaxRetries()), 1);
        retryBaseWaitTime = (long) conf.get(Ors2Config.retryBaseWaitTime());
        long networkSlowTime = (long) conf.get(Ors2Config.networkSlowTime());
        mapWriteDispersion = (boolean) conf.get(Ors2Config.mapWriteDispersion());
        int workerRetryNumber = (int) conf.get(Ors2Config.workerRetryNumber());
        this.taskInfo = taskInfo;
        semaphore = new Semaphore((int) conf.get(Ors2Config.maxFlyingPackageNum()));
        this.clientFactory = clientFactory;

        logger.info("NettyClient create success. ioThreads = {} , netWorkTimeout = {} ms, ioMaxRetry = {} times, retryBaseWaitTime = {} ms, networkSlowTime = {} ms",
                clientFactory.getIoThreads(), netWorkTimeout, ioMaxRetry, retryBaseWaitTime, networkSlowTime);

        this.serverGroup = new Ors2ServerSwitchGroup(groupList, taskInfo.getMapId(), workerRetryNumber, mapWriteDispersion);

        callback = new ResponseCallback() {
            @Override
            public void onSuccess(Request request) {
                semaphore.release();
                sendFinish.incrementAndGet();
            }

            public boolean exceedMaxRetryNumber(int retry) {
                if (retry >= ioMaxRetry) {
                    String msg = String.format("write for task %s data send fail, retries exceeding the maximum limit of %s times",
                            taskInfo.getMapId(), ioMaxRetry);
                    clientFactory.setException(new Ors2NetworkException(msg));
                    sendFinish.incrementAndGet();
                    semaphore.release();
                    return true;
                } else {
                    return false;
                }
            }

            @Override
            public void onFailure(Request request, Throwable e) {
                if (exceedMaxRetryNumber(request.getRetry())) {
                    return;
                }

                Request retryRequest = createRetryRequest(request, false);
                long wait = getWaitTime(retryRequest.getRetry());

                logger.warn("write for task {} data send fail: {}, retry the {} time, wait {} mills, id {}",
                        taskInfo.getMapId(), e.getMessage(), retryRequest.getRetry(), wait, request.id());

                clientFactory.schedule(retryRequest::writeBuild, wait, TimeUnit.MILLISECONDS);
            }

            @Override
            public void onError(Request request, Throwable e) {
                if (exceedMaxRetryNumber(request.getRetry())) {
                    return;
                }

                Request retryRequest = createRetryRequest(request, true);

                logger.warn("write for task {} data send network error: {}, retry the {} time , id {}",
                        taskInfo.getMapId(), e.getMessage(), retryRequest.getRetry(), request.id());

                retryRequest.writeBuild();
            }
        };
    }

    public ShuffleClient getBuildClient(Ors2WorkerDetail server) {
        return clientFactory.getBuildClient(server);
    }

    public ShuffleClient getDataClient(Ors2WorkerDetail server) {
        return clientFactory.getDataClient(server);
    }

    public void send(int workerId, ShufflePacket packet) {
        clientFactory.checkNetworkException();

        try {
            semaphore.acquire();

            Tuple2<ShuffleClient, ShuffleClient> tuple2 = getClient(workerId, 0, Optional.empty());
            Request request = new Request(packet, workerId, tuple2._1, tuple2._2, callback);

            readySend.incrementAndGet();
            request.writeBuild();
        }catch (Exception e) {
            logger.error("Send package to shuffle worker failed: ", e);
            throw new RuntimeException(e);
        }
    }

    public void close() {
        // pass
    }

    public void waitFinish() {
        int v = getRemainPackageNum();
        SleepWaitTimeout waitTimeout = new SleepWaitTimeout(clientFactory.getNetWorkTimeout());

        while (v != 0) {
            clientFactory.checkNetworkException();
            try {
                waitTimeout.sleepAdd(50);
                v = getRemainPackageNum();
                //logger.info("Waiting for the data packet to be sent, the current remaining: {}.", v);
            } catch (TimeoutException e) {
                throw new RuntimeException("Waiting for the request to complete timed out", e);
            }
        }
    }

    public int getFinishPackageNum() {
        return sendFinish.get();
    }

    public int getRemainPackageNum() {
        return readySend.get() - sendFinish.get();
    }

    public long getWaitTime(int nextRetry) {
        int i = RandomUtils.nextInt(1, 11);
        return i * retryBaseWaitTime;
    }

    public static String requestId() {
        return  "ors2_" + Math.abs(UUID.randomUUID().getLeastSignificantBits());
    }

    public Request createRetryRequest(Request request, boolean channelError) {
        int retry = request.addRetry();
        Optional<Ors2WorkerDetail> errorServer;
        if (channelError) {
            errorServer = Optional.of(request.getServer());
        } else {
            errorServer = Optional.empty();
        }

        Tuple2<ShuffleClient, ShuffleClient> tuple2 = getClient(request.getWorkerId(), retry, errorServer);
        return new Request(request.getPacket(), request.getWorkerId(), tuple2._1, tuple2._2, request.getCallback());
    }

    public Ors2ClientFactory getClientFactory() {
        return clientFactory;
    }

    public Tuple2<ShuffleClient, ShuffleClient> getClient(int workerId, int retry, Optional<Ors2WorkerDetail> errorServer) {
        int loopTimes = serverGroup.availableSize(workerId);

        RuntimeException lastException = null;
        Ors2WorkerDetail useServer = serverGroup.getServer(workerId, retry, errorServer);
        for (int i = 0; i < loopTimes; i++) {
            try {
                ShuffleClient buildClient = getBuildClient(useServer);
                ShuffleClient dataClient = getDataClient(useServer);
                return Tuple2.apply(buildClient, dataClient);
            } catch (RuntimeException e) {
                lastException = e;
                logger.error("connect server {} fail: {}", useServer, e.getMessage());
                useServer = serverGroup.getServer(workerId, retry, Optional.of(useServer));
            }
        }

        if (lastException != null) {
            clientFactory.setException(lastException);
            throw lastException;
        } else {
            return null;
        }
    }
}
