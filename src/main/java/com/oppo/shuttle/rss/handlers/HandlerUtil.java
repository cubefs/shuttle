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

package com.oppo.shuttle.rss.handlers;

import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HandlerUtil {
    private static final Logger logger = LoggerFactory.getLogger(HandlerUtil.class);

    public static ChannelFuture writeResponseMsg(ChannelHandlerContext ctx, byte responseStatus,
                                                 Message msg, boolean doWriteType, int messageType) {
        // need to serialize msg to get its length
        ByteBuf responseMsgBuf = ctx.alloc().buffer(1000);
        try {
            writeBytes(responseMsgBuf, msg, doWriteType, true, responseStatus, messageType);
            return ctx.writeAndFlush(responseMsgBuf);
        } catch (Throwable ex) {
            logger.warn("Caught exception, releasing ByteBuf", ex);
            responseMsgBuf.release();
            throw ex;
        }
    }

    public static ChannelFuture writeRequestMsg(Channel channel, Message msg, int messageType) {
        ByteBuf msgBuf = channel.alloc().buffer(1000);
        try {
            writeBytes(msgBuf, msg, true, false, (byte) 0, messageType);
            return channel.writeAndFlush(msgBuf);
        } catch (Throwable ex) {
            logger.warn("Caught exception, releasing ByteBuf", ex);
            msgBuf.release();
            throw ex;
        }
    }

    public static void writeBytes(ByteBuf msgBuf, Message msg, boolean doWriteType,
                                  boolean doWriteStatus, byte status, int messageType) {
        byte[] bytes = msg.toByteArray();
        if (doWriteStatus) {
            msgBuf.writeByte(status);
        }
        if (doWriteType) {
            msgBuf.writeInt(messageType);
        }
        msgBuf.writeInt(bytes.length);
        msgBuf.writeBytes(bytes);
    }
}
