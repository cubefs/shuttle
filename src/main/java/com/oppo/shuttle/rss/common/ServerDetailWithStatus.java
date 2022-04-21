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

package com.oppo.shuttle.rss.common;

import com.oppo.shuttle.rss.messages.ShuffleMessage;

import java.util.concurrent.atomic.AtomicBoolean;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Store ShuffleWorker status in ShuffleMaster
 * Uses to monitor and manage ShuffleWorker status by ShuffleMaster
 *
 * @author oppo
 */

@JsonIgnoreProperties({"serverDetail", "shuffleWorkerHealthInfo"})
public class ServerDetailWithStatus {

    private Ors2WorkerDetail serverDetail;
    private final AtomicBoolean isOnLine = new AtomicBoolean(true);
    private int loadWeight = 1;

    private ShuffleMessage.ShuffleWorkerHealthInfo shuffleWorkerHealthInfo;
    private int selfCheckOkTimes;
    private long punishStartTime;
    private long punishDuration;
    private final AtomicBoolean isBlack = new AtomicBoolean(false);
    private final AtomicBoolean isPunished = new AtomicBoolean(false);
    private volatile boolean isInBlackConf = false;

    public ServerDetailWithStatus(Ors2WorkerDetail serverDetail, int loadWeight) {
        this.serverDetail = serverDetail;
        this.loadWeight = loadWeight;
    }

    public ShuffleMessage.ShuffleWorkerHealthInfo getShuffleWorkerHealthInfo() {
        return shuffleWorkerHealthInfo;
    }

    public void setShuffleWorkerHealthInfo(ShuffleMessage.ShuffleWorkerHealthInfo shuffleWorkerHealthInfo) {
        this.shuffleWorkerHealthInfo = shuffleWorkerHealthInfo;
    }

    public boolean isInBlackList() {
        return isBlack.get();
    }

    public boolean isPunished() {
        return isPunished.get();
    }

    public boolean isOnLine() {
        return isOnLine.get();
    }

    public Ors2WorkerDetail getServerDetail() {
        return serverDetail;
    }

    public int getLoadWeight() {
        return loadWeight;
    }

    public void addToBlackList() {
        isBlack.getAndSet(true);
        isOnLine.getAndSet(false);
        selfCheckOkTimes = 0;
    }

    public void refreshBlackConf(){
        addToBlackList();
        isInBlackConf = true;
    }

    public void removeFromBlackList() {
        isBlack.getAndSet(false);
        isOnLine.getAndSet(true);
        if (isInBlackConf){
            isInBlackConf = false;
        }
    }

    public void addToPunishList(long punishStartTime, long punishDuration) {
        isPunished.getAndSet(true);
        isOnLine.getAndSet(false);
        this.punishStartTime = punishStartTime;
        this.punishDuration = punishDuration;
    }

    public void removeFromPunishList() {
        isPunished.getAndSet(false);
        isOnLine.getAndSet(true);
    }

    public int incrementAndGetOkTimes() {
        return ++selfCheckOkTimes;
    }

    public boolean isPunishFinished(long currentTime) {
        return isPunished.get() && currentTime > punishStartTime + punishDuration;
    }

    public boolean isHeartbeatExpired(long currentTime, long interval) {
        if (shuffleWorkerHealthInfo == null){
            return false;
        }
        return currentTime - shuffleWorkerHealthInfo.getUpdateTime() > interval;
    }

    public void doublePunishTime() {
        punishDuration *= 2;
    }

    public boolean isInBlackConf() {
        return isInBlackConf;
    }

    @Override
    public String toString() {
        return "ServerDetailWithStatus{" +
                "serverDetail=" + serverDetail +
                ", isOnLine=" + isOnLine +
                ", loadWeight=" + loadWeight +
                ", shuffleWorkerHealthInfo=" + shuffleWorkerHealthInfo +
                ", selfCheckOkTimes=" + selfCheckOkTimes +
                ", punishStartTime=" + punishStartTime +
                ", punishDuration=" + punishDuration +
                ", isBlack=" + isBlack +
                ", isPunished=" + isPunished +
                ", isInBlackConf=" + isInBlackConf +
                '}';
    }
}