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

package com.oppo.shuttle.rss.server.master;

import com.oppo.shuttle.rss.util.JsonUtils;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.netty.buffer.Unpooled.copiedBuffer;

public class ShuffleMasterHttpHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(ShuffleMasterHttpHandler.class);

    private final AtomicBoolean isLeader;

    private String leaderHostAndPort;

    private static final String WORKERS = "/workers";

    public ShuffleMasterHttpHandler(AtomicBoolean isLeader, String leaderHostAndPort) {
        this.isLeader = isLeader;
        this.leaderHostAndPort = leaderHostAndPort;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof FullHttpRequest) {
            final FullHttpRequest request = (FullHttpRequest) msg;

            HttpResponseStatus status;
            String responseMessage;
            if (WORKERS.equals(request.uri())) {
                responseMessage = JsonUtils.objToJson(ShuffleWorkerStatusManager.reportMetrics());
                status = HttpResponseStatus.OK;
            } else {
                responseMessage = String.format("%s not found", request.uri());
                status = HttpResponseStatus.NOT_FOUND;
            }

            String redirectUrl = "http://" + leaderHostAndPort + request.uri();
            if (!isLeader.get()) {
                status = HttpResponseStatus.TEMPORARY_REDIRECT;
            }

            FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status,
                    copiedBuffer(responseMessage.getBytes()));

            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, responseMessage.length());
            response.headers().set(HttpHeaderNames.LOCATION, redirectUrl);

            if (HttpUtil.isKeepAlive(request)) {
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
                ctx.writeAndFlush(response, ctx.voidPromise());
            } else {
                ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
            }

        } else {
            super.channelRead(ctx, msg);
        }
    }



    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.warn("HTTPHandler got exception", cause);
        ctx.writeAndFlush(new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR,
                copiedBuffer(cause.getMessage().getBytes(StandardCharsets.UTF_8))
        )).addListener(ChannelFutureListener.CLOSE);
    }
}
