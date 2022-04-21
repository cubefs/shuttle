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

package com.oppo.shuttle.rss.metadata;

import com.oppo.shuttle.rss.clients.Ors2GetWorkersClient;
import com.oppo.shuttle.rss.messages.ShuffleMessage;
import com.oppo.shuttle.rss.messages.ShuffleWorkerStorageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Manage shuffle worker for shuffle master
 * @author oppo
 */
public class Ors2MasterServerManager implements ServiceManager {
    private static final Logger logger = LoggerFactory.getLogger(Ors2MasterServerManager.class);

    private Ors2GetWorkersClient ors2GetWorkersClient;
    private final String servers;

    public Ors2MasterServerManager(
            ZkShuffleServiceManager zkManager,
            long timeoutMillis,
            long retryInterval,
            String masterName,
            boolean useEpoll) {
        this.servers = zkManager.getZkServerConnStr();
        ors2GetWorkersClient = new Ors2GetWorkersClient(zkManager, timeoutMillis, retryInterval, masterName, useEpoll);
    }

    @Override
    public void close() {
        if (ors2GetWorkersClient != null){
            ors2GetWorkersClient.close();
        }
        logger.info("Closed Ors2MasterServerManager successful");
    }

    @Override
    public String toString() {
        return new StringBuilder("Ors2MasterServiceRegistry{servers=").append(servers).append("}").toString();
    }

    @Override
    public void registerServer(String dataCenter, String cluster, String hostIpPort, ServerRole role, ShuffleWorkerStorageType storageType) {

    }

    @Override
    public void removeServer(String dataCenter, String cluster, ServerRole role) {

    }

    @Override
    public List<String> getServers(String dataCenter, String cluster, int count, ServerRole role,
                                   ShuffleWorkerStorageType storageType) {
        return null;
    }

    @Override
    public ShuffleMessage.GetWorkersResponse getServersWithConf(
            String dataCenter,
            String cluster,
            int maxCount,
            int jobPriority,
            String appId,
            String dagId,
            String taskId,
            String appName) {
        ShuffleMessage.GetWorkersRequest getWorkersRequest = ShuffleMessage.GetWorkersRequest.newBuilder()
                .setDagId(dagId)
                .setAppId(appId)
                .setJobPriority(jobPriority)
                .setRequestWorkerCount(maxCount)
                .setCluster(cluster)
                .setDataCenter(dataCenter)
                .setTaskId(taskId)
                .setAppName(appName)
                .build();
        return ors2GetWorkersClient.getServers(getWorkersRequest);
    }
}
