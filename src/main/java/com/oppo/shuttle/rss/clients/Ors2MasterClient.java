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

import com.oppo.shuttle.rss.common.HostPortInfo;
import com.oppo.shuttle.rss.decoders.Ors2MasterClientDecoder;
import com.oppo.shuttle.rss.exceptions.Ors2IOException;
import com.oppo.shuttle.rss.exceptions.Ors2InvalidAddressException;
import com.oppo.shuttle.rss.exceptions.Ors2NetworkException;
import com.oppo.shuttle.rss.metadata.ZkShuffleServiceManager;
import com.oppo.shuttle.rss.handlers.HandlerUtil;
import com.oppo.shuttle.rss.util.CommonUtils;
import com.oppo.shuttle.rss.util.NetworkUtils;
import com.google.protobuf.Message;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.timeout.WriteTimeoutHandler;
import io.netty.util.concurrent.Future;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicLong;

public class Ors2MasterClient implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(Ors2MasterClient.class);

    private static final AtomicLong internalClientIdSeed = new AtomicLong(0);

    private final ZkShuffleServiceManager zkShuffleServiceManager;
    private EventLoopGroup clientLoopGroup;
    private Bootstrap driverClient;
    private Channel channel;
    private final long timeoutMillis;
    private final long retryInterval;
    private final String masterName;
    private final boolean useEpoll;
    private NodeCache nodeCache;
    private String connectionInfo;

    private Ors2MasterClientDecoder ors2MasterClientDecoder = new Ors2MasterClientDecoder();

    private final long internalClientId = internalClientIdSeed.getAndIncrement();

    // TODO: refine as singleton instance
    public Ors2MasterClient(
            ZkShuffleServiceManager zkShuffleServiceManager,
            long timeoutMillis,
            long retryInterval,
            String masterName,
            boolean useEpoll) {
        this.zkShuffleServiceManager = zkShuffleServiceManager;
        this.masterName = masterName;
        this.timeoutMillis = timeoutMillis;
        this.retryInterval = retryInterval;
        this.useEpoll = useEpoll;
        this.connectionInfo = String.format("%s %s [%s get zookeeper: %s]", this.getClass().getSimpleName(),
                internalClientId, NetworkUtils.getLocalIp(), masterName);
        init();
        logger.debug("Created instance (timeout: {} millis): {}", timeoutMillis, this);
    }

    public void init() {
        // TODO: code refine
        try {
            clientLoopGroup = useEpoll ? new EpollEventLoopGroup(1) : new NioEventLoopGroup(1);
            HostPortInfo hostAndPort = zkShuffleServiceManager.getMaster(masterName);
            driverClient = createBootstrap();
            channel = connectToMaster(hostAndPort).channel();
            connectionInfo = String.format("%s %s connect to master [%s -> %s:%s]",
                    this.getClass().getSimpleName(),
                    internalClientId,
                    NetworkUtils.getLocalIp(),
                    hostAndPort.getHost(),
                    hostAndPort.getPort());

            nodeCache = zkShuffleServiceManager.getNodeCache(masterName);
            nodeCache.start(true);
            nodeCache.getListenable().addListener(this::changeChannel);
        } catch (Exception e) {
            logger.error("Connect to master failed, connectionInfo: {}", connectionInfo, e);
            throw new Ors2NetworkException(e);
        }
    }

    private Bootstrap createBootstrap() {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(clientLoopGroup).channel(useEpoll ? EpollSocketChannel.class : NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) timeoutMillis)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new WriteTimeoutHandler(10));
                        ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(256 * 1024, 5, 4));
                        ch.pipeline().addLast(ors2MasterClientDecoder);
                    }
                });
        return bootstrap;
    }

    private ChannelFuture connectToMaster(HostPortInfo hostAndPort) {
        if (hostAndPort.getHost() == null) {
            throw new Ors2InvalidAddressException("Can't get shuffle master host from zk");
        }
        final InetSocketAddress address = new InetSocketAddress(hostAndPort.getHost(), hostAndPort.getPort());
        return CommonUtils.retry(retryInterval, timeoutMillis,
                () -> {
            ChannelFuture cf = driverClient.connect(address);
            try {
                if (!cf.await(timeoutMillis)){
                    throw new Ors2IOException(
                            String.format("Connecting to shuffle master %s timed out (%s ms)", address, timeoutMillis));
                }
            } catch (InterruptedException | Ors2IOException e) {
                Thread.currentThread().interrupt();
                logger.error("Waiting connect to master interrupted: ", e);
                return null;
            }
            return cf;
        });
    }

    public void writeMsg(Message msg, int messageType) {
        HandlerUtil.writeRequestMsg(channel, msg, messageType);
    }

    public void changeChannel() {
        try {
            closeChannel();
            ChildData currentData = nodeCache.getCurrentData();
            byte[] currentMasterInfo = currentData.getData();
            HostPortInfo hostAndPort = HostPortInfo.parseFromStr(new String(currentMasterInfo));
            logger.info("Active master has changed to {}:{}", hostAndPort.getHost(), hostAndPort.getPort());
            channel = connectToMaster(hostAndPort).channel();
        } catch (Throwable t) {
            logger.error("Watch active master node change event error: ", t);
        }
    }

    private void closeChannel() {
        try {
            if (channel != null) {
                channel.closeFuture().sync();
            }
        } catch (Throwable ex) {
            logger.warn("Failed to close driver client channel future", ex);
        }
    }

    public Channel getChannel() {
        return channel;
    }

    public NodeCache getNodeCache() {
        return nodeCache;
    }

    public Ors2MasterClientDecoder getWorkerClientRegistryDecoder() {
        return ors2MasterClientDecoder;
    }

    @Override
    public String toString() {
        return connectionInfo;
    }

    @Override
    public void close() {
        Future<?> clientFuture = clientLoopGroup.shutdownGracefully();
        try {
            clientFuture.get();
        } catch (Throwable ex) {
            logger.warn("Hit exception when shutting down driver client event loop group", ex);
        }

        closeChannel();

        if (zkShuffleServiceManager != null) {
            zkShuffleServiceManager.close();
        }
    }

}
