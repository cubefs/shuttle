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

public class Ors2ServerStatistics {

    private String dataCenter;
    private String cluster;
    private int workersTotal;
    private int normalNum;
    private int blackNum;
    private int punishNum;
    private ServerListDir serverListDetail;

    public Ors2ServerStatistics(String dataCenter, String cluster, int workersTotal, int normalNum, int blackNum, int punishNum, ServerListDir serverListDetail) {
        this.dataCenter = dataCenter;
        this.cluster = cluster;
        this.workersTotal = workersTotal;
        this.normalNum = normalNum;
        this.blackNum = blackNum;
        this.punishNum = punishNum;
        this.serverListDetail = serverListDetail;
    }

    public String getDataCenter() {
        return dataCenter;
    }

    public String getCluster() {
        return cluster;
    }

    public int getWorkersTotal() {
        return workersTotal;
    }

    public int getNormalNum() {
        return normalNum;
    }

    public int getBlackNum() {
        return blackNum;
    }

    public int getPunishNum() {
        return punishNum;
    }

    public ServerListDir getServerListDetail() {
        return serverListDetail;
    }
}
