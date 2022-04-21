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

import com.oppo.shuttle.rss.common.Pair;
import com.oppo.shuttle.rss.exceptions.Ors2InvalidDataException;
import com.oppo.shuttle.rss.execution.BuildConnectionExecutor;
import com.oppo.shuttle.rss.messages.MessageConstants;
import com.oppo.shuttle.rss.messages.ShuffleMessage;
import com.oppo.shuttle.rss.util.NetworkUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BuildConnectionInboundHandler extends ChannelInboundHandlerAdapter {
  private static final Logger logger = LoggerFactory.getLogger(BuildConnectionInboundHandler.class);

  private final String serverId;
  private final String runningVersion;
  private final long idleTimeoutMillis;
  private final BuildConnectionExecutor connectionExecutor;

  private String connectionInfo = "";

  public BuildConnectionInboundHandler(String serverId, String runningVersion, long idleTimeoutMillis,
        BuildConnectionExecutor connectionExecutor) {
    this.serverId = serverId;
    this.runningVersion = runningVersion;
    this.idleTimeoutMillis = idleTimeoutMillis;
    this.connectionExecutor = connectionExecutor;
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    super.channelActive(ctx);
    processChannelActive(ctx);
  }

  public void processChannelActive(final ChannelHandlerContext ctx) {
    connectionInfo = NetworkUtils.getConnectInfo(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    super.channelInactive(ctx);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    long now = System.currentTimeMillis();

    if (!(msg instanceof ShuffleMessage.BuildConnectionRequest)) {
      throw new Ors2InvalidDataException(String.format("Unsupported message: %s, %s", msg, connectionInfo));
    }

    ShuffleMessage.BuildConnectionRequest connectionRequest = (ShuffleMessage.BuildConnectionRequest)msg;
    Pair<Integer, Long> buildResult = new Pair<>(-1, -1L);

    int buildStatus = connectionExecutor.buildConnection(connectionRequest, buildResult);

    ShuffleMessage.BuildConnectionResponse response = ShuffleMessage
            .BuildConnectionResponse
            .newBuilder()
            .setControlType(ShuffleMessage.FlowControlType.forNumber(buildStatus))
            .setId(buildResult.getKey())
            .setValue(buildResult.getValue())
            .setMessageId(connectionRequest.getMessageId())
            .setResponseTime(System.currentTimeMillis())
            .build();

    logger.debug("BuildConnectionResponse  messageId: {}, since create: {}, since send: {}, " +
                    "processTime: {}, id: {}, value: {}, connInfo: {}",
            connectionRequest.getMessageId(),
            now - connectionRequest.getCreateTime(),
            now - connectionRequest.getSendTime(),
            System.currentTimeMillis() - now,
            buildResult.getKey(),
            buildResult.getValue(),
            connectionInfo);

    HandlerUtil.writeResponseMsg(ctx, MessageConstants.RESPONSE_STATUS_OK, response, true,
            MessageConstants.MESSAGE_BuildConnectionResponse);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    String msg = "Got exception " + connectionInfo;
    logger.error(msg, cause);
    ctx.close();
  }
}