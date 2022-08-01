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
import com.oppo.shuttle.rss.common.Pair;
import com.oppo.shuttle.rss.common.ServerDetailWithStatus;
import com.oppo.shuttle.rss.common.ServerListDir;
import com.oppo.shuttle.rss.exceptions.Ors2Exception;
import com.oppo.shuttle.rss.metrics.Ors2MetricsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author oppo
 */
public abstract class ShuffleMasterDispatcher {

    private static final Logger logger = LoggerFactory.getLogger(ShuffleMasterDispatcher.class);

    private final ShuffleServerConfig masterConfig;

    private Random rand = new Random();

    protected ShuffleMasterDispatcher(ShuffleServerConfig masterConfig) {
        this.masterConfig = masterConfig;
    }

    public abstract MasterDispatchServers getServerList(int requestCount,
                                                        String dataCenter,
                                                        String cluster,
                                                        String dagId,
                                                        int numPartitions);

    protected MasterDispatchServers getAvailableServers(
            String dataCenter,
            String cluster,
            String dagId) {
        // get dagId conf from zk
        Map<String, Pair<String,String>> dagCluster = ShuffleMasterClusterManager.getDagCluster();
        if (dagCluster.containsKey(dagId)){
            Pair<String, String> dataCenterCluster = dagCluster.get(dagId);
            dataCenter = dataCenterCluster.getKey();
            cluster = dataCenterCluster.getValue();
            logger.info("Zk conf: dag id is {}, dataCenter is {}, cluster is {}", dagId, dataCenter, cluster);
        }

        Map<String, Map<String, ServerListDir>> workerList = ShuffleWorkerStatusManager.getWorkerList();
        if (!workerList.containsKey(dataCenter)){
            dataCenter = masterConfig.getDataCenter();
        }
        Map<String, ServerListDir> serverListDirMap = workerList.get(dataCenter);
        if (serverListDirMap != null && !serverListDirMap.containsKey(cluster)){
            cluster = masterConfig.getCluster();
        }

        Collection<ServerDetailWithStatus> allServers = tryToGetServers(dataCenter, cluster);
        List<ServerDetailWithStatus> candidateSeverList = new ArrayList<>();
        ServerListDir serverListDir = new ServerListDir(ShuffleServerConfig.DEFAULT_ROOT_DIR, "");
        if (allServers == null) {
            throw new Ors2Exception(String.format("dataCenter: %s, cluster %s no worker available", dataCenter, cluster));
        }

        if (!allServers.isEmpty()){
            serverListDir = ShuffleWorkerStatusManager.getWorkerList().get(dataCenter).get(cluster);
            candidateSeverList = allServers
                    .stream()
                    .filter(ServerDetailWithStatus::isOnLine)
                    .collect(Collectors.toList());
        }

        if (Ors2MetricsConstants.workerNum.labels(dataCenter, cluster, "blacklist").get() +
                Ors2MetricsConstants.workerNum.labels(dataCenter, cluster, "punish").get() >
                Ors2MetricsConstants.workerNum.labels(dataCenter, cluster, "normal").get()){
            throw new Ors2Exception("Too many unhealthy workers, refuse the request.");
        }

        return new MasterDispatchServers(dataCenter, cluster, serverListDir.getRootDir(), candidateSeverList, serverListDir.getFsConf());
    }

    private Collection<ServerDetailWithStatus> tryToGetServers(String dataCenter, String cluster){
        Map<String, Map<String, ServerListDir>> workerList = ShuffleWorkerStatusManager.getWorkerList();
        if (workerList.containsKey(dataCenter) && workerList.get(dataCenter).containsKey(cluster)){
            return workerList.get(dataCenter).get(cluster).getHostStatusMap().values();
        }
        return null;
    }


}
