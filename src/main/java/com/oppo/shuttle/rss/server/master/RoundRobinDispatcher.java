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

import com.oppo.shuttle.rss.ShuffleServerConfig;
import com.oppo.shuttle.rss.common.MasterDispatchServers;
import com.oppo.shuttle.rss.common.Ors2WorkerDetail;
import com.oppo.shuttle.rss.common.ServerDetailWithStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class RoundRobinDispatcher extends ShuffleMasterDispatcher {

    private static final Logger logger = LoggerFactory.getLogger(RoundRobinDispatcher.class);

    private static int startIdx = 0;

    public RoundRobinDispatcher(ShuffleServerConfig masterConfig) {
        super(masterConfig);
    }

    @Override
    public synchronized MasterDispatchServers getServerList(
            int requestCount,
            String dataCenter,
            String cluster,
            String dagId,
            int jobPriority) {

        MasterDispatchServers masterDispatchServers =
                getAvailableServers(dataCenter, cluster, dagId);
        List<Ors2WorkerDetail> serverList =
                masterDispatchServers.getCandidates()
                        .stream()
                        .map(ServerDetailWithStatus::getServerDetail)
                        .collect(Collectors.toList());
        if (serverList.isEmpty() || requestCount > serverList.size()) {
            masterDispatchServers.setServerDetailList(serverList);
            return masterDispatchServers;
        }

        int serverSize = serverList.size();
        List<Ors2WorkerDetail> resultServerList;
        String onlineServers = serverList.stream()
                .map(Ors2WorkerDetail::getServerId)
                .collect(Collectors.joining(","));
        if (startIdx >= serverSize) {
            startIdx = serverSize - 1;
        }
        if (logger.isDebugEnabled()) {
            logger.info("Current online servers are: {}, startIdx is {}.", onlineServers, startIdx);
        }
        int endIdx = startIdx + requestCount;
        if (endIdx <= serverSize) {
            resultServerList = serverList.subList(startIdx, endIdx);
        } else {
            endIdx %= serverSize;
            resultServerList = serverList.subList(startIdx, serverSize);
            resultServerList.addAll(serverList.subList(0, endIdx));
        }
        startIdx = endIdx;
        masterDispatchServers.setServerDetailList(resultServerList);
        return masterDispatchServers;
    }

}
