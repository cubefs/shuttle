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

package com.oppo.shuttle.rss.clients.handler;

import com.oppo.shuttle.rss.clients.ShuffleClient;
import com.oppo.shuttle.rss.exceptions.Ors2Exception;
import com.oppo.shuttle.rss.exceptions.Ors2ShuffleBusyException;
import com.oppo.shuttle.rss.messages.MessageConstants;
import com.oppo.shuttle.rss.messages.ShuffleMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BuildConnectionHandler extends SimpleChannelInboundHandler<ByteBuf> {
    private static final Logger logger = LoggerFactory.getLogger(BuildConnectionHandler.class);
    private final long networkSlowTime;
    private final ShuffleClient client;

    public BuildConnectionHandler(long networkSlowTime, ShuffleClient client) {
        this.networkSlowTime = networkSlowTime;
        this.client = client;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        byte status = msg.readByte();
        if (status != MessageConstants.RESPONSE_STATUS_OK) {
            throw new Ors2Exception("Server corresponding exception, status: " + status);
        }

        int messageType = msg.readInt();
        msg.skipBytes(4);
        if (messageType != MessageConstants.MESSAGE_BuildConnectionResponse) {
            throw new Ors2Exception("Can not found this messageType: " + messageType);
        }

        byte[] bytes = new byte[msg.readableBytes()];
        msg.readBytes(bytes);
        ShuffleMessage.BuildConnectionResponse message = ShuffleMessage.BuildConnectionResponse.parseFrom(bytes);
        long responseTime = message.getResponseTime();
        String msgId = message.getMessageId();
        Request request = client.removeRequest(msgId);

        if (message.getControlType() == ShuffleMessage.FlowControlType.NO_CONTROL) {
            long costTime = responseTime - request.getStart();
            if (costTime >= networkSlowTime) {
                logger.warn("build conn to {} so slow, cost {} millis, id {}, val {},  messageId {}",
                        client.address(), costTime, message.getId(), message.getValue(), message.getMessageId());
            }
            request.writeData(message.getId(), message.getValue());
        } else {
            String desc;
            if (message.getControlType() == ShuffleMessage.FlowControlType.BUSY_CONTROL) {
                desc = "BUSY_CONTROL";
            } else if (message.getControlType() == ShuffleMessage.FlowControlType.MEM_CONTROL) {
                desc = "MEM_CONTROL";
            } else {
                desc = "UNKNOWN";
            }
            request.onFailure(new Ors2ShuffleBusyException(desc + " on " +  client.address()));
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            if (e.state() == IdleState.ALL_IDLE) {
               if (client.numPendingRequests() > 0) {
                   logger.warn("The connection has timed out, but there are still unprocessed requests.");
               } else {
                   logger.info("Close idle connection {}", client.address());
                   client.timeout();
                   ctx.close();
               }
            }
        }  else {
            ctx.fireUserEventTriggered(evt);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("Exception in connection from " + client.address(), cause);
        client.handleFailure(cause);
        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (client.numPendingRequests() > 0) {
            String msg = String.format("Channel %s closed, but there are no outstanding requests", client.address());
            client.handleFailure(new Ors2Exception(msg));
        }
    }
}
