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
import com.oppo.shuttle.rss.clients.handler.ShuffleWriteHandler;
import com.oppo.shuttle.rss.common.Ors2WorkerDetail;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.CountDownLatch;

public class ShuffleClient implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(ShuffleClient.class);

    private volatile boolean timeout;

    private final Channel channel;

    private final Ors2WorkerDetail server;

    private final ShuffleWriteHandler handler;

    public ShuffleClient(Ors2WorkerDetail server, Channel channel, ShuffleWriteHandler handler) {
        this.channel = channel;
        this.handler = handler;
        this.server = server;
    }

    public void timeout() {
        timeout = true;
    }

    public boolean isActive() {
        return !timeout && (channel.isOpen() || channel.isActive());
    }

    public SocketAddress address() {
        return channel.remoteAddress();
    }

    public ChannelFuture writeAndFlush(Request request, ByteBuf buf) {
        handler.addRequest(request);
        return channel.writeAndFlush(buf);
    }

    public Request getRequest(String id) {
        return handler.getRequest(id);
    }

    public Request removeRequest(String id) {
        return handler.removeRequest(id);
    }

    public int numPendingRequests() {
        return handler.numPendingRequests();
    }

    public void handleFailure(Throwable cause) {
        handler.handleFailure(cause);
    }

    public Ors2WorkerDetail getServer() {
        return server;
    }

    @Override
    public void close() throws IOException {
        close(null);
    }

    public void close(CountDownLatch latch) {
        channel.close().addListener((future -> {
            if (latch != null) {
                latch.countDown();
            }
            if (!future.isSuccess()) {
                logger.warn("channel close fail", future.cause());
            }
        }));
    }
}
