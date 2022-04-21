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
import com.oppo.shuttle.rss.common.Ors2ServerStatistics;
import com.oppo.shuttle.rss.common.Ors2WorkerDetail;
import com.oppo.shuttle.rss.common.ServerDetailWithStatus;
import com.oppo.shuttle.rss.common.ServerListDir;
import com.oppo.shuttle.rss.messages.ShuffleMessage.ShuffleWorkerHealthInfo;
import com.oppo.shuttle.rss.metrics.Ors2MetricsConstants;
import com.oppo.shuttle.rss.util.ScheduledThreadPoolUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class ShuffleWorkerStatusManager {

    private static final Logger logger = LoggerFactory.getLogger(ShuffleWorkerStatusManager.class);

    /**
     * worker list with status group by type
     */
    private static final Map<String, Map<String, ServerListDir>> WORKER_LIST = new ConcurrentHashMap<>();
    private final ShuffleServerConfig masterConfig;
    private final BlackListRefresher blackListRefresher;
    private final long workerCheckInterval;

    private final ShuffleDataDirClear dataDirClear;

    public ShuffleWorkerStatusManager(ShuffleServerConfig masterConfig, BlackListRefresher blackListRefresher) {
        this.masterConfig = masterConfig;
        this.blackListRefresher = blackListRefresher;
        this.workerCheckInterval = masterConfig.getWorkerCheckInterval();
        this.dataDirClear = new ShuffleDataDirClear(masterConfig.getAppStorageRetentionMillis());
        updateStart();
    }

    public void updateStart() {
        logger.info("start ShuffleWorkerStatusManager, workerCheckInterval: {} mills, appFileRetentionMillis: {} mills",
                masterConfig.getWorkerCheckInterval(), masterConfig.getAppStorageRetentionMillis());
        ScheduledThreadPoolUtils.scheduleAtFixedRate(
                this::updateWorkerStatus,
                masterConfig.getUpdateDelay(),
                workerCheckInterval);

        ScheduledThreadPoolUtils.scheduleAtFixedRate(
                this::clearShuffleDirRunnable,
                masterConfig.getUpdateDelay(),
                masterConfig.getClearShuffleDirInterval());

    }

    // timing check worker status
    private void updateWorkerStatus() {
        long currentTime = System.currentTimeMillis();
        getWorkerStatusStream()
                .map(Map::values)
                .flatMap(Collection::stream)
                .forEach(serverDetailWithStatus -> {
                    if (serverDetailWithStatus.isPunishFinished(currentTime)) {
                        if (isWorkerBusy(serverDetailWithStatus.getShuffleWorkerHealthInfo())) {
                            serverDetailWithStatus.doublePunishTime();
                        } else {
                            serverDetailWithStatus.removeFromPunishList();
                            logger.info("{} punish end, set it online", serverDetailWithStatus.getServerDetail());
                        }
                    }

                    if (serverDetailWithStatus.isHeartbeatExpired(currentTime, workerCheckInterval) &&
                            serverDetailWithStatus.isOnLine()) {
                        serverDetailWithStatus.addToBlackList();
                        logger.info("Haven't receive heartbeat from {} in {} seconds, set it offline",
                                serverDetailWithStatus.getServerDetail(), workerCheckInterval / 1000);
                    }
                });
        reportMetrics();
    }

    public void updateHealthInfo(ShuffleWorkerHealthInfo workerHealthInfo) {
        String hostIp = workerHealthInfo.getHostIp();
        int shufflePort = workerHealthInfo.getShufflePort();
        String serverId = getServerId(hostIp, shufflePort);
        if (isServerInBlackListConf(serverId)) {
            return;
        }

        //separate workers by dataCenter and cluster
        ServerDetailWithStatus serverDetailWithStatus = getServerDetailWithStatus(workerHealthInfo, hostIp, shufflePort, serverId);

        //handle black worker
        if (!workerHealthInfo.getSelfCheckOK() && !serverDetailWithStatus.isInBlackList()) {
            serverDetailWithStatus.addToBlackList();
            logger.info("{} has been added into black list!", serverId);
        } else if (serverDetailWithStatus.isInBlackList()) {
            int selfCheckOkTimes = serverDetailWithStatus.incrementAndGetOkTimes();
            if (selfCheckOkTimes >= masterConfig.getWorkerSelfCheckOkTimes()) {
                serverDetailWithStatus.removeFromBlackList();
                logger.info("{} self check ok for {} times, remove it from black list!",
                        serverId, masterConfig.getWorkerSelfCheckOkTimes());
            }
        }

        // handle busy worker
        if (isWorkerBusy(workerHealthInfo) && !serverDetailWithStatus.isPunished()) {
            long punishMill = masterConfig.getWorkerPunishMills();
            serverDetailWithStatus.addToPunishList(System.currentTimeMillis(), punishMill);
            logger.info("{} is busy, add it to punish list!", serverId);
        }
    }

    private ServerDetailWithStatus getServerDetailWithStatus(
            ShuffleWorkerHealthInfo workerHealthInfo,
            String hostIp,
            int shufflePort,
            String serverId) {
        Map<String, ServerListDir> clusterMap = WORKER_LIST.computeIfAbsent(
                workerHealthInfo.getDataCenter(), 
                t -> new ConcurrentHashMap<>(4));
        ServerListDir serverListDir = clusterMap.computeIfAbsent(
                workerHealthInfo.getCluster(), 
                t -> new ServerListDir(workerHealthInfo.getRootDirectory(), workerHealthInfo.getFsConf()));
        ServerDetailWithStatus serverDetailWithStatus =
                new ServerDetailWithStatus(
                        new Ors2WorkerDetail(hostIp, shufflePort, workerHealthInfo.getBuildConnPort()),
                        workerHealthInfo.getWorkerLoadWeight());
        Map<String, ServerDetailWithStatus> hostStatusMap = serverListDir.getHostStatusMap();
        hostStatusMap.put(serverId, serverDetailWithStatus);
        serverDetailWithStatus.setShuffleWorkerHealthInfo(workerHealthInfo);
        return serverDetailWithStatus;
    }

    public static void unregisterWorker(String serverId) {
        logger.info("{} unregister from master", serverId);
        getWorkerStatusStream().forEach(t -> t.remove(serverId));
    }

    public static void setToBlackStatus(String serverId) {
        getWorkerStatusStream().forEach(t -> {
            if (t.containsKey(serverId)) {
                t.get(serverId).refreshBlackConf();
            }
        });
    }

    private static Stream<Map<String, ServerDetailWithStatus>> getWorkerStatusStream() {
        return WORKER_LIST.values()
                .stream()
                .map(Map::values)
                .flatMap(Collection::stream)
                .map(ServerListDir::getHostStatusMap);
    }


    private String getServerId(String ip, int port) {
        return String.format("%s_%d", ip, port);
    }

    private boolean isServerInBlackListConf(String serverId) {
        List<String> workerBlackList = blackListRefresher.getWorkerBlackList();
        return workerBlackList != null && !workerBlackList.isEmpty() && workerBlackList.contains(serverId);
    }

    private boolean isWorkerBusy(ShuffleWorkerHealthInfo workerHealthInfo) {
        return workerHealthInfo.getThroughputPerMin() > masterConfig.getMaxThroughputPerMin()
                || workerHealthInfo.getRejectConnNum() > masterConfig.getMaxFlowControlTimes()
                || workerHealthInfo.getHoldDataSize() > masterConfig.getMaxHoldDataSize();
    }

    public static Map<String, Map<String, ServerListDir>> getWorkerList() {
        return WORKER_LIST;
    }

    public void clearShuffleDirRunnable() {
        try {
            WORKER_LIST.values()
                    .stream()
                    .flatMap(x -> x.values().stream())
                    .forEach(dataDirClear::execute);
        } catch (Exception e) {
            logger.error("clearShuffleDirRunnable execute failed", e);
        }
    }

    public static List<Ors2ServerStatistics> reportMetrics() {
        List<Ors2ServerStatistics> ors2ServerStatistics = new ArrayList<>();
        for (Map.Entry<String, Map<String, ServerListDir>> dcEntry : WORKER_LIST.entrySet()) {
            for (Map.Entry<String, ServerListDir> clusterEntry : dcEntry.getValue().entrySet()) {
                String dc = dcEntry.getKey();
                String cluster = clusterEntry.getKey();
                int totalNum = 0;
                int normalNum = 0;
                int blackListNum = 0;
                int punishListNum = 0;
                Map<String, ServerDetailWithStatus> hostStatusMap = clusterEntry.getValue().getHostStatusMap();
                for (ServerDetailWithStatus serverDetailWithStatus : hostStatusMap.values()) {
                    totalNum++;
                    if (serverDetailWithStatus.isOnLine()) {
                        normalNum++;
                    } else if (serverDetailWithStatus.isInBlackList() && !serverDetailWithStatus.isInBlackConf()) {
                        blackListNum++;
                    } else if (serverDetailWithStatus.isPunished()) {
                        punishListNum++;
                    }
                }
                logger.info("{} {} current worker normalNum is {}", dc, cluster, normalNum);
                Ors2MetricsConstants.workerNum.labels(dc, cluster, "normal").set(normalNum);
                Ors2MetricsConstants.workerNum.labels(dc, cluster, "blacklist").set(blackListNum);
                Ors2MetricsConstants.workerNum.labels(dc, cluster, "punish").set(punishListNum);
                ors2ServerStatistics.add(new Ors2ServerStatistics(dc, cluster, totalNum, normalNum, blackListNum, punishListNum, clusterEntry.getValue()));
            }
        }
        return ors2ServerStatistics;
    }

}
