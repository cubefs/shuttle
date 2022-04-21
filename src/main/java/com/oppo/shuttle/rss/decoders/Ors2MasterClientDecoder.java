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

package com.oppo.shuttle.rss.decoders;

import com.oppo.shuttle.rss.exceptions.Ors2InvalidDataException;
import com.oppo.shuttle.rss.handlers.MsgHandleUtil;
import com.oppo.shuttle.rss.messages.MessageConstants;
import com.oppo.shuttle.rss.messages.ShuffleMessage;
import com.oppo.shuttle.rss.util.NetworkUtils;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

@ChannelHandler.Sharable
public class Ors2MasterClientDecoder extends SimpleChannelInboundHandler<ByteBuf> {

    private static final Logger logger = LoggerFactory.getLogger(Ors2MasterClientDecoder.class);

    private CountDownLatch latch;
    private Message result;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf in) {
        if (in.readableBytes() < Integer.BYTES) {
            logger.warn("ReadableBytes not enough: {}", in.readableBytes());
            return;
        }
        try {
            decodeMsg(ctx, in);
        } catch (Exception e) {
            logger.error("Got exception when decode message, ", e);
            throw e;
        }
    }

    private void decodeMsg(ChannelHandlerContext ctx, ByteBuf inBuf) {
        if (inBuf.readableBytes() == 0) {
            return;
        }
        try {
            MsgHandleUtil.checkResponseStatus(inBuf);
            int messageId = inBuf.readInt();
            byte[] bytes = MsgHandleUtil.readMsg(inBuf);
            switch (messageId) {
                case MessageConstants.MESSAGE_SHUFFLE_RESPONSE_INFO:
                    result = ShuffleMessage.GetWorkersResponse.parseFrom(bytes);
                    break;
                default:
                    throw new Ors2InvalidDataException(String.format("Message id: %s is invalid.", messageId));
            }
        } catch (Ors2InvalidDataException | InvalidProtocolBufferException e) {
            String clientInfo = NetworkUtils.getConnectInfo(ctx);
            logger.error("Message type error: ", e);
            ctx.close();
            logger.info(String.format("Closed connection to client %s", clientInfo));
        } finally {
            latch.countDown();
        }
    }

    public Message getResult() {
        return result;
    }

    public void resetLatch(CountDownLatch latch) {
        this.latch = latch;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.warn(cause.getMessage());
        ctx.close();
    }

}
