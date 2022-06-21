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

import com.oppo.shuttle.rss.common.Constants;
import com.oppo.shuttle.rss.common.StageShuffleId;
import com.oppo.shuttle.rss.exceptions.Ors2InvalidDataException;
import com.oppo.shuttle.rss.execution.BuildConnectionExecutor;
import com.oppo.shuttle.rss.execution.ShuffleDataExecutor;
import com.oppo.shuttle.rss.messages.MessageConstants;
import com.oppo.shuttle.rss.messages.ShuffleData;
import com.oppo.shuttle.rss.messages.ShuffleMessage;
import com.oppo.shuttle.rss.util.NetworkUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleDataIncomingHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(ShuffleDataIncomingHandler.class);

    private final String serverId;
    private String shuffleConnInfo = "";
    private String appId = null;

    private final ShuffleDataExecutor executor;
    private final BuildConnectionExecutor buildConnExecutor;

    public ShuffleDataIncomingHandler(String serverId,
      ShuffleDataExecutor executor, BuildConnectionExecutor buildConnExecutor) {
        this.serverId = serverId;
        this.executor = executor;
        this.buildConnExecutor = buildConnExecutor;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        long now = System.currentTimeMillis();
        // Process other messages. We assume the header messages are already processed,
        // thus some fields of this
        if (!(msg instanceof ShuffleData)) {
            throw new Ors2InvalidDataException(String.format("Unsupported message: %s, %s", msg, shuffleConnInfo));
        }

        ShuffleData shuffleData = (ShuffleData) msg;
        ShuffleMessage.UploadPackageRequest up = shuffleData.getUploadPackage();

        if (up.getBuildId() != Constants.SKIP_CHECK_BUILD_ID) {
            if(!buildConnExecutor.checkConnectionIdValue(up.getBuildId(), up.getBuildValue())) {
                logger.warn("invalid or timeout connectionId&Value: {}, {}, for {}, address {}",
                        up.getBuildId(), up.getBuildValue(), up.getAppId(), shuffleConnInfo);
            } else {
                buildConnExecutor.releaseConnection(up.getBuildId());
            }
        }

        if (appId == null) {
            appId = up.getAppId();
        }

        StageShuffleId stageShuffleId = new StageShuffleId(appId, up.getAppAttempt(),
                up.getStageAttempt(),
                up.getShuffleId());

        executor.initStageSpace(stageShuffleId);

        ShuffleMessage.UploadPackageResponse uploadResponse = ShuffleMessage
                .UploadPackageResponse
                .newBuilder()
                .setServerId(serverId)
                .setMessageId(up.getMessageId())
                .setResponseTime(System.currentTimeMillis())
                .build();

        try {
            executor.updateAppAliveness(appId);
            executor.processUploadPackage(ctx, shuffleData, stageShuffleId, shuffleConnInfo);
            HandlerUtil.writeResponseMsg(ctx, MessageConstants.RESPONSE_STATUS_OK,
                    uploadResponse, true, MessageConstants.MESSAGE_UploadPackageResponse);

            logger.debug("UploadPackage messageId: {}, since create: {}, since send: {}, processTime: {}",
                    up.getMessageId(), now - up.getCreateTime(), now - up.getSendTime(), System.currentTimeMillis() - now);
        } catch (Throwable e) {
            logger.warn("Process UploadPackage request fail", e);
            HandlerUtil.writeResponseMsg(ctx, MessageConstants.RESPONSE_STATUS_SERVER_BUSY,
                    uploadResponse, true, MessageConstants.MESSAGE_UploadPackageResponse);
        }
    }
    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        processChannelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }

    public void processChannelActive(final ChannelHandlerContext ctx) {
        shuffleConnInfo = NetworkUtils.getConnectInfo(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        String msg = "Got exception with " + shuffleConnInfo;
        logger.error(msg, cause);
        ctx.close();
    }
}
