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

import com.oppo.shuttle.rss.messages.ShuffleMessage;
import com.oppo.shuttle.rss.messages.ShuffleWorkerStorageType;

import java.util.List;

/**
 * Manage shuffle worker, register / get shuffle worker
 * Manage shuffle master, register to zk
 *
 * @author oppo
 */
public interface ServiceManager extends AutoCloseable {

    enum ServerRole {
        SS_WORKER,
        SS_MASTER,
        SS_MASTER_HA,
        SS_MASTER_ACTIVE,
        SS_NONE
    }

    String DEFAULT_DATA_CENTER = "SSD_HDFS";

    // This is only used in tests and test tools. The actual default is calculated at runtime
    String DEFAULT_TEST_CLUSTER = "default";

    String DEFAULT_MASTER = "default_master";

    /**
     * Register shuffle master/worker
     *
     * @param dataCenter
     * @param cluster
     * @param hostIpPort  master[ip:masterPort], worker[ip:dataPort:connPort]
     * @param role        specified the role of server to register
     * @param storageType shuffle worker storage type
     */
    void registerServer(String dataCenter, String cluster, String hostIpPort, ServerRole role, ShuffleWorkerStorageType storageType);

    /**
     * Remove server from zk or master normally
     *
     * @param dataCenter
     * @param cluster
     * @param role
     */
    void removeServer(String dataCenter, String cluster, ServerRole role);

    /**
     * Get shuffle master/worker
     *
     * @param dataCenter
     * @param cluster
     * @param count
     * @param role
     * @return server ip and ports
     */
    List<String> getServers(String dataCenter, String cluster, int count, ServerRole role, ShuffleWorkerStorageType storageType);


    /**
     * Get shuffle master/worker
     *
     * @param dataCenter
     * @param cluster
     * @param maxCount
     * @param jobPriority
     * @param appId
     * @param dagId
     * @param taskId
     * @param appName
     * @return server list with conf
     */
    ShuffleMessage.GetWorkersResponse getServersWithConf(
            String dataCenter,
            String cluster,
            int maxCount,
            int jobPriority,
            String appId,
            String dagId,
            String taskId,
            String appName);
}
