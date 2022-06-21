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
import com.oppo.shuttle.rss.common.Constants;
import com.oppo.shuttle.rss.common.Ors2WorkerDetail;
import com.oppo.shuttle.rss.messages.ShufflePacket;
import org.apache.spark.util.Utils;

public class Request {
    private final boolean flowControlEnable;
    private final ShufflePacket packet;
    private final ShuffleClient buildClient;
    private final ShuffleClient dataClient;

    private final ResponseCallback callback;

    private final long start = System.currentTimeMillis();

    private final int workerId;

    public Request(
            boolean flowControlEnable,
            ShufflePacket packet,
            int workerId,
            ShuffleClient buildClient,
            ShuffleClient dataClient,
            ResponseCallback callback) {
        this.flowControlEnable = flowControlEnable;
        this.packet = packet;
        this.buildClient = buildClient;
        this.dataClient = dataClient;
        this.callback = callback;
        this.workerId = workerId;
    }

    public ResponseCallback getCallback() {
        return callback;
    }

    public long getStart() {
        return start;
    }

    public void onFailure(Throwable e) {
        callback.onFailure(this, e);
    }

    public void onSuccess() {
        callback.onSuccess(this);
    }

    public void onError(Throwable e) {
        callback.onError(this, e);
    }

    public void writeBuild() {
        if (flowControlEnable) {
            buildClient.writeAndFlush(this, packet.buildBuffer());
        } else {
            writeData(Constants.SKIP_CHECK_BUILD_ID, Constants.SKIP_CHECK_BUILD_ID);
        }
    }

    public void writeData(int id, long value) {
        dataClient.writeAndFlush(this, packet.dataBuffer(id, value));
    }

    public String id() {
        return packet.getId();
    }

    public String dataSizeString() {
        return Utils.bytesToString(packet.getDataSize());
    }

    public int getRetry() {
        return packet.getBuildBuilder().getRetryIdx();
    }

    public synchronized int addRetry() {
        int next = packet.getBuildBuilder().getRetryIdx() + 1;
        packet.getBuildBuilder().setRetryIdx(next);
        return next;
    }

    public int getWorkerId() {
        return workerId;
    }

    public Ors2WorkerDetail getServer() {
        return dataClient.getServer();
    }

    public ShufflePacket getPacket() {
        return packet;
    }
}