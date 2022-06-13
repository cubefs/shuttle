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

import com.oppo.shuttle.rss.common.MasterDispatchServers;
import com.oppo.shuttle.rss.common.Ors2WorkerDetail;
import com.oppo.shuttle.rss.handlers.HandlerUtil;
import com.oppo.shuttle.rss.handlers.MsgHandleUtil;
import com.oppo.shuttle.rss.messages.MessageConstants;
import com.oppo.shuttle.rss.messages.ShuffleMessage.*;
import com.oppo.shuttle.rss.metrics.Ors2MetricsConstants;
import com.oppo.shuttle.rss.util.NetworkUtils;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class ShuffleMasterHandler extends SimpleChannelInboundHandler<ByteBuf> {
    private static final Logger logger = LoggerFactory.getLogger(ShuffleMasterHandler.class);
    private final ShuffleMasterDispatcher shuffleMasterDispatcher;
    private final ShuffleWorkerStatusManager shuffleWorkerStatusManager;
    private final ApplicationRequestController applicationRequestController;

    public ShuffleMasterHandler(ShuffleMasterDispatcher shuffleMasterDispatcher,
                                ShuffleWorkerStatusManager shuffleWorkerStatusManager,
                                ApplicationRequestController applicationRequestController) {
        this.shuffleMasterDispatcher = shuffleMasterDispatcher;
        this.shuffleWorkerStatusManager = shuffleWorkerStatusManager;
        this.applicationRequestController = applicationRequestController;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        if (in.readableBytes() < Integer.BYTES) {
            logger.warn("ReadableBytes not enough: {}", in.readableBytes());
            return;
        }
        try {
            handleMsg(ctx, in);
        } catch (Exception e) {
            logger.error("Got exception when decode message, ", e);
            throw e;
        }
    }

    private void handleMsg(ChannelHandlerContext ctx, ByteBuf in) throws InvalidProtocolBufferException {
        int msgName = in.readInt();
        byte[] bytes = MsgHandleUtil.readMsg(in);
        switch (msgName) {
            case MessageConstants.MESSAGE_SHUFFLE_WORKER_HEALTH_INFO:
                ShuffleWorkerHealthInfo shuffleWorkerHealthInfo = ShuffleWorkerHealthInfo.parseFrom(bytes);
                handleHealthInfo(shuffleWorkerHealthInfo);
                break;

            case MessageConstants.MESSAGE_DRIVER_REQUEST_INFO:
                GetWorkersRequest getWorkersRequest = GetWorkersRequest.parseFrom(bytes);
                handleDriverRequest(getWorkersRequest, ctx);
                break;

            case MessageConstants.MESSAGE_SHUFFLE_WORKER_UNREGISTER_REQUEST:
                ShuffleWorkerUnregisterRequest shuffleWorkerUnregisterRequest = ShuffleWorkerUnregisterRequest.parseFrom(bytes);
                handleUnregisterRequest(shuffleWorkerUnregisterRequest);
                break;

            default: {
                String clientInfo = NetworkUtils.getConnectInfo(ctx);
                logger.warn("Invalid message number {} from client {}", msgName, clientInfo);
                ctx.close();
                logger.info("Closed connection to client {}", clientInfo);
                break;
            }
        }
    }

    private void handleHealthInfo(ShuffleWorkerHealthInfo workerHealthInfo) {
        shuffleWorkerStatusManager.updateHealthInfo(workerHealthInfo);
    }

    private void handleDriverRequest(GetWorkersRequest getWorkersRequest, ChannelHandlerContext ctx) {
        logger.info("Received driver request from {}, request is {}", ctx.channel().remoteAddress(), getWorkersRequest);
        String appName = "".equals(getWorkersRequest.getTaskId()) ?
                getWorkersRequest.getAppName() : getWorkersRequest.getTaskId();
        if (!applicationRequestController.requestCome(appName, getWorkersRequest.getAppId())){
            HandlerUtil.writeResponseMsg(ctx,
                    MessageConstants.RESPONSE_STATUS_OK,
                    GetWorkersResponse.newBuilder().setIsSuccess(false).build(),
                    true,
                    MessageConstants.MESSAGE_SHUFFLE_RESPONSE_INFO);
            return;
        }

        MasterDispatchServers masterDispatchServers = shuffleMasterDispatcher.getServerList(
                getWorkersRequest.getRequestWorkerCount(),
                getWorkersRequest.getDataCenter(),
                getWorkersRequest.getCluster(),
                getWorkersRequest.getDagId(),
                getWorkersRequest.getJobPriority());
        List<Ors2WorkerDetail> serverList = masterDispatchServers.getServerDetailList();

        // count each worker distributed times
        serverList.forEach(t -> Ors2MetricsConstants.workerDistributeTimes.labels(t.getServerId()).inc());

        logger.debug("Got {} workers for request: {}", serverList.size(), serverList.stream().map(Ors2WorkerDetail::toString).collect(Collectors.toList()));
        GetWorkersResponse response;
        response = GetWorkersResponse.newBuilder()
                .addAllSeverDetail(serverList.stream().map(Ors2WorkerDetail::convertToProto).collect(Collectors.toList()))
                .setDataCenter(masterDispatchServers.getDataCenter())
                .setCluster(masterDispatchServers.getCluster())
                .setRootDir(masterDispatchServers.getRootDir())
                .setFileSystemConf(masterDispatchServers.getFsConf())
                .setIsSuccess(true)
                .build();
        HandlerUtil.writeResponseMsg(ctx, MessageConstants.RESPONSE_STATUS_OK, response, true, MessageConstants.MESSAGE_SHUFFLE_RESPONSE_INFO);
    }

    private void handleUnregisterRequest(ShuffleWorkerUnregisterRequest shuffleWorkerUnregisterRequest){
        ShuffleWorkerStatusManager.unregisterWorker(shuffleWorkerUnregisterRequest.getServerId());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        String msg = "Got exception in master handle process: " + NetworkUtils.getConnectInfo(ctx);
        logger.warn(msg, cause);
        ctx.close();
    }
}