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

package com.oppo.shuttle.rss.server;

import com.oppo.shuttle.rss.ShuffleServerConfig;
import com.oppo.shuttle.rss.common.Constants;
import com.oppo.shuttle.rss.util.ScheduledThreadPoolUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import org.apache.hadoop.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

/**
 * Parent class of ShuffleMaster and ShuffleWorker
 * Abstract the common functions for both servers type
 * @author oppo
 */
public abstract class ShuffleServer {
    private static final Logger logger = LoggerFactory.getLogger(ShuffleServer.class);

    public static Thread addShutdownHook(ShuffleServer server) {
        Thread shutdownHook = new Thread(() -> {
            server.shutdown();
            logger.info("Shutdown server in shutdown hook");
        });

        ShutdownHookManager.get().addShutdownHook(shutdownHook, Constants.SERVER_SHUTDOWN_PRIORITY);
        return shutdownHook;
    }

    protected enum ChannelType {
        WORKER_DATA_CHANNEL,
        WORKER_BUILD_CONN_CHANNEL,
        MASTER_AGGREGATE_CHANNEL,
        MASTER_HTTP_CHANNEL
    }

    public EventLoopGroup initEventLoopGroups(boolean useEpoll, int theadCount,  String threadPrefix) {
        ThreadFactory threadFactory = new DefaultThreadFactory(threadPrefix, true);

        if (useEpoll) {
            // use epoll nio mode
            return new EpollEventLoopGroup(theadCount, threadFactory);
        } else {
            // use nio mode
            return  new NioEventLoopGroup(theadCount, threadFactory);
        }
    }


    /**
     * Bind port to ServerBootstrap
     * @param bootstrap
     * @param port
     * @return ServerBootstrap channel and port
     * @throws InterruptedException
     */
    protected Channel bindPort(ServerBootstrap bootstrap, int port) throws InterruptedException {
        Channel channel = bootstrap.bind(port).sync().channel();
        logger.info("Bound bootstrap specified port: {}",  port);
        return channel;
    }

    /**
     * Init ServerBootstrap for ShuffleMaster and ShuffleWorker
     * @param parentGroup
     * @param childGroup
     * @param serverConfig
     * @param handlerSupplier
     * @return
     */
    protected ServerBootstrap initServerBootstrap(EventLoopGroup parentGroup,
                                                  EventLoopGroup childGroup,
                                                  ShuffleServerConfig serverConfig,
                                                  Supplier<ChannelHandler[]> handlerSupplier) {
        logger.info("UseEpoll {} ", serverConfig.isUseEpoll());

        ServerBootstrap bootstrap = serverConfig.isUseEpoll() ?
                new ServerBootstrap().group(parentGroup, childGroup)
                        .channel(EpollServerSocketChannel.class)
                : new ServerBootstrap().group(parentGroup, childGroup)
                .channel(NioServerSocketChannel.class);

        return bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(final SocketChannel ch) {
                ch.pipeline().addLast(handlerSupplier.get());
            }
        })
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, serverConfig.getNetworkTimeout())
                .option(ChannelOption.SO_BACKLOG, serverConfig.getNetworkBacklog())
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, serverConfig.getNetworkTimeout())
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
                // .childOption(ChannelOption.SO_RCVBUF, 128 * 1024)
                // .childOption(ChannelOption.SO_SNDBUF, 128 * 1024);
    }

    /**
     * Close channels of ShuffleServer
     * @param channels
     */
    protected void closeChannels(List<Channel> channels) {
        for (Channel channel : channels) {
            try {
                channel.close();
            } catch (Exception e) {
                logger.error("Shutdown channel exception, {}", channel.toString(), e);
            }
        }
        logger.info("Channels closed");
    }

    /**
     * Shutdown ShuffleServer's netty eventLoopGroup gracefully
     * @param eventLoopGroup
     * @param loopName
     */
    protected void shutdownEventLoopGroup(EventLoopGroup eventLoopGroup, String loopName) {
        try {
            Future<?> future = eventLoopGroup.shutdownGracefully();
            future.get();
        } catch (Exception ex) {
            logger.error("Shutdown shuffle event loop group : {} exception",loopName, ex);
        }
        logger.info("Event loop group shutdown gracefully finished: {}.", loopName);
    }

    /**
     * Init ShuffleServer configs and managers
     */
    protected abstract void init();

    /**
     * Init ShuffleServer netty channel instance and configuration
     * @param serverId
     * @param type
     * @throws InterruptedException
     */
    protected abstract void initChannel(String serverId, ChannelType type) throws InterruptedException;

    /**
     * Concat the id for ShuffleServer
     * @return
     */
    public abstract String getServerId();

    /**
     * Shutdown ShuffleServer, closing channel, shutdown netty eventLoopGroup
     */
    protected void shutdown(){
        ScheduledThreadPoolUtils.shutdown();
    }

    /**
     * Run the server
     * @throws InterruptedException
     */
    protected abstract void run() throws InterruptedException;


}
