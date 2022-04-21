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

import com.oppo.shuttle.rss.clients.handler.BuildConnectionHandler;
import com.oppo.shuttle.rss.clients.handler.ShuffleWriteHandler;
import com.oppo.shuttle.rss.clients.handler.UploadPackageHandler;
import com.oppo.shuttle.rss.common.Ors2WorkerDetail;
import com.oppo.shuttle.rss.exceptions.Ors2Exception;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.Ors2Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class Ors2ClientFactory {
    private static final Logger logger = LoggerFactory.getLogger(Ors2ClientFactory.class);

    private final Class<? extends Channel> channelClass;
    private final EventLoopGroup workerGroup;
    private final int ioThreads;
    private final long netWorkTimeout;
    private final long networkSlowTime;
    private final int ioMaxRetry;
    private final long ioRetryWait;
    private final int numConnections;
    private final boolean ioErrorResend;

    private final Random rand;

    private volatile boolean closed = false;

    private final ConcurrentHashMap<InetSocketAddress, Ors2ClientPool> connectionPool = new ConcurrentHashMap<>();

    private final AtomicReference<Throwable> futureException = new AtomicReference<>(null);

    public static final String TYPE_BUILD = "build";
    public static final String TYPE_DATA = "data";


    public Ors2ClientFactory(SparkConf conf) {
        ioThreads = (int) conf.get(Ors2Config.ioThreads());
        netWorkTimeout = (long) conf.get(Ors2Config.networkTimeout());
        networkSlowTime = (long) conf.get(Ors2Config.networkSlowTime());
        ioMaxRetry = (int) conf.get(Ors2Config.ioMaxRetry());
        ioRetryWait = (long) conf.get(Ors2Config.ioRetryWait());
        numConnections = (int) conf.get(Ors2Config.numConnections());
        ioErrorResend = (boolean) conf.get(Ors2Config.ioErrorResend());
        rand = new Random();

        channelClass = getClientChannelClass();
        workerGroup = createEventLoop(ioThreads, "ors2-shuffle-client");
    }

    public Bootstrap createBootstrap(Ors2WorkerDetail server, AtomicReference<ShuffleClient> channelRef, String type) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
                .channel(channelClass)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) netWorkTimeout)
                .option(ChannelOption.SO_SNDBUF, 128 * 1024)
                .option(ChannelOption.SO_RCVBUF, 128 * 1024)
                .handler(new ChannelInitializer<SocketChannel>() {
                    protected void initChannel(SocketChannel ch) throws Exception {
                        LengthFieldBasedFrameDecoder decoder = new LengthFieldBasedFrameDecoder(134217728, 5, 4);
                        ch.pipeline().addLast(decoder);
                        ch.pipeline().addLast(new IdleStateHandler(0, 0, (int) netWorkTimeout / 1000));

                        ShuffleWriteHandler handler = new ShuffleWriteHandler(futureException::set, ioErrorResend);
                        ShuffleClient dataClient = new ShuffleClient(server, ch, handler);
                        ch.pipeline().addLast(createHandler(type, dataClient));

                        channelRef.set(dataClient);
                    }
                });

        return bootstrap;
    }

    public static Class<? extends Channel> getClientChannelClass() {
        return NioSocketChannel.class;
    }

    public static EventLoopGroup createEventLoop(int numThreads, String threadPrefix) {
        assert numThreads > 0;
        ThreadFactory threadFactory =  new DefaultThreadFactory(threadPrefix, true);
        return new NioEventLoopGroup(numThreads, threadFactory);
    }

    public void schedule(Runnable command, long delay, TimeUnit unit) {
        workerGroup.schedule(command, delay, unit);
    }

    public int getIoThreads() {
        return ioThreads;
    }

    public ShuffleClient getClient(Ors2WorkerDetail server, String type) throws Exception{
        InetSocketAddress address = createAddress(server, type);

        Ors2ClientPool clientPool = connectionPool.computeIfAbsent(address, key -> new Ors2ClientPool(numConnections));

        int clientIndex = rand.nextInt(numConnections);
        ShuffleClient client = clientPool.clients[clientIndex];
        if (client != null && client.isActive()) {
            return client;
        }

        synchronized (clientPool.locks[clientIndex]) {
            client = clientPool.clients[clientIndex];
            if (client != null && client.isActive()) {
                return client;
            }

            long preConnect = System.currentTimeMillis();
            final AtomicReference<ShuffleClient> clientRef = new AtomicReference<>();
            Bootstrap bootstrap = createBootstrap(server, clientRef, type);
            ChannelFuture cf = bootstrap.connect(address);

            if (!cf.await(netWorkTimeout)) {
                throw new IOException(String.format("Connecting to %s(index %s) timed out (%s ms)",
                        address, clientIndex, netWorkTimeout));
            } else if (cf.cause() != null) {
                throw new IOException(String.format("Failed to connect to %s", address), cf.cause());
            }

            client = clientRef.get();
            clientPool.clients[clientIndex] = client;

            logger.info("Successfully connection to ors2 server {}(index {}) after {} ms" ,
                    address, clientIndex, System.currentTimeMillis() - preConnect);

            return client;
        }
    }

    public ShuffleClient getClientRetry(Ors2WorkerDetail server, String type) {
        Exception lastException = null;
        for (int i = 0; i < ioMaxRetry; i++) {
            try {
                return getClient(server, type);
            } catch (Exception e) {
                lastException = e;
                logger.warn("connect to {} fail, retry {}, error message: {}", createAddress(server, type), i, e.getMessage());
                if (i != 0) {
                    try {
                        Thread.sleep(ioRetryWait);
                    } catch (InterruptedException ie) {
                        // pass
                    }
                }
            }
        }

        if (lastException != null) {
            throw new Ors2Exception(String.format("connect to %s fail", createAddress(server, type)), lastException);
        } else {
            return null;
        }
    }

    public ShuffleClient getBuildClient(Ors2WorkerDetail server) {
        return getClientRetry(server, TYPE_BUILD);
    }

    public ShuffleClient getDataClient(Ors2WorkerDetail server) {
        return getClientRetry(server, TYPE_DATA);
    }

    public void setException(Throwable e) {
        futureException.set(e);
    }

    public void checkNetworkException() {
        Throwable throwable = futureException.get();
        if (throwable != null) {
            if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else {
                throw new RuntimeException(throwable);
            }
        }
    }

    public synchronized void stop() {
        if (closed) {
            return;
        }
        closed = true;

        int clientSize = connectionPool.size() * numConnections;
        CountDownLatch latch = new CountDownLatch(clientSize);
        long start = System.currentTimeMillis();

        for (Ors2ClientPool clientPool : connectionPool.values()) {
            for (int i = 0; i < clientPool.clients.length; i++) {
                ShuffleClient client = clientPool.clients[i];
                if (client != null) {
                    clientPool.clients[i] = null;
                    client.close(latch);
                }
            }
        }
        connectionPool.clear();

        try {
            latch.await();
            if (workerGroup != null && !workerGroup.isShuttingDown()) {
                workerGroup.shutdownGracefully();
            }
            long closeTime = System.currentTimeMillis() - start;
            logger.info("close client {}, cost {} mills", clientSize, closeTime);
        } catch (InterruptedException e) {
            logger.warn("latch.await", e);
        }
    }

    public SimpleChannelInboundHandler<?> createHandler(String type, ShuffleClient client) {
        switch (type) {
            case TYPE_BUILD:
                return new BuildConnectionHandler(networkSlowTime, client);
            case TYPE_DATA:
                return new UploadPackageHandler(networkSlowTime, client);
            default:
                throw new Ors2Exception();
        }
    }

    public InetSocketAddress createAddress(Ors2WorkerDetail server, String type) {
        switch (type) {
            case TYPE_BUILD:
                return server.buildAddress();
            case TYPE_DATA:
                return server.dataAddress();
            default:
                throw new Ors2Exception();
        }
    }


    public long getNetWorkTimeout() {
        return netWorkTimeout;
    }
}
