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

import com.oppo.shuttle.rss.common.HostPortInfo;
import com.oppo.shuttle.rss.common.Ors2WorkerDetail;
import com.oppo.shuttle.rss.exceptions.Ors2Exception;
import com.oppo.shuttle.rss.exceptions.Ors2IllegalArgumentException;
import com.oppo.shuttle.rss.messages.ShuffleMessage;
import com.oppo.shuttle.rss.messages.ShuffleWorkerStorageType;
import com.oppo.shuttle.rss.util.JsonUtils;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Use Zk path to store shuffle worker info: host and port
 * zk path include info: data center and cluster name
 *
 * @author oppo
 */
public class ZkShuffleServiceManager implements ServiceManager {
    private static final Logger logger = LoggerFactory.getLogger(ZkShuffleServiceManager.class);

    private static final String ZK_RSS_BASE_PATH = "shuffle_rss_path";
    private static final String ZK_WORKER_SUBPATH = "shuffle_worker";
    private static final String ZK_MASTER_SUBPATH = "shuffle_master";
    private static final String ZK_MASTER_HA_PATH = "shuffle_master_ha";
    private static final String ZK_MASTER_WATCH_PATH = "shuffle_master_watch";
    public static final String ZK_STORAGE_DAG_PATH = "shuffle_storage_type";
    public static final String ZK_DAG_ID_PATH = "dag_id";

    private final String zkServerConnStr;
    private final int timeoutMs;
    private final int baseSleepTimeMs;
    private final int maxSleepTimeMs;

    /**
     * Network connection max retry times
     */
    private final int maxRetries;

    private CuratorFramework zkCurator;


    public ZkShuffleServiceManager(String zkServerConnStr, int timeoutMs, int maxRetries) {
        this(zkServerConnStr, timeoutMs, 1000, maxRetries, 10000);
    }

    public ZkShuffleServiceManager(String zkServerConnStr, int timeoutMs, int baseSleepTimeMs, int maxRetries, int maxSleepMs) {
        this.zkServerConnStr = zkServerConnStr;
        this.timeoutMs = timeoutMs;
        this.baseSleepTimeMs = baseSleepTimeMs;
        this.maxSleepTimeMs = maxSleepMs;
        this.maxRetries = maxRetries;

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries, maxSleepMs);
        zkCurator = CuratorFrameworkFactory.newClient(zkServerConnStr, timeoutMs, timeoutMs, retryPolicy);
        zkCurator.start();
        logger.info("Init ZkShuffleWorkerManager: " + this);
    }

    public void setData(String path, byte[] data) {
        try {
            zkCurator.setData().forPath(path, data);
        } catch (Exception e) {
            throw new Ors2Exception("set data Exception: " + e.getCause());
        }
    }

    public byte[] getZkNodeData(String nodePath) {
        byte[] bytes = null;
        try {
            bytes = zkCurator.getData().forPath(nodePath);
        } catch (Exception e) {
            throw new Ors2Exception("Failed to get node data for zk node path: " + nodePath, e);
        }
        if (bytes == null) {
            throw new Ors2Exception("Data is null for zk node path:" + nodePath);
        }
        return bytes;
    }

    /**
     * Check zk node exists or need covered
     */
    private boolean isNecessaryCreedNode(String nodePath, CreateMode createMode) {
        boolean res = false;
        try {
            if (zkCurator.checkExists().forPath(nodePath) == null
                    || createMode == CreateMode.EPHEMERAL) {
                res = true;
            }
        } catch (Exception e) {
            logger.error("Unnecessary to create zk node", e);
            throw new Ors2Exception("Unnecessary to create zk node", e);
        }
        return res;
    }

    /**
     * Create zk node for shuffle master or worker
     *
     * @param nodePath
     * @param nodeData
     * @param mode
     */
    public void createNode(String nodePath, byte[] nodeData, CreateMode mode) {

        assert this.maxRetries > 1 : "Create zk node maxRetries must > 1!";

        // check nodePath exists or the create mode is zk node will be deleted upon the client's disconnect
        if (isNecessaryCreedNode(nodePath, mode)) {
            logger.info("Trying to create zk node: {}", nodePath);

            int tryTimes = 0;
            while (tryTimes < this.maxRetries) {
                try {
                    zkCurator.create().creatingParentsIfNeeded()
                            .withMode(mode).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                            .forPath(nodePath, nodeData);
                    break;
                } catch (KeeperException.NodeExistsException nodeExistsException) {
                    logger.info("Zk node already exists, try to delete it: {}", nodePath);
                    try {
                        zkCurator.delete().forPath(nodePath);
                    } catch (Exception e) {
                        logger.error("Delete existing zk node failed: " + nodePath, e);
                    }
                } catch (Exception e) {
                    logger.error("Exception creating zk node for: " + nodePath, e);
                }
                tryTimes++;
            }
            if (tryTimes == this.maxRetries) {
                throw new Ors2Exception("Exceeds maxRetries times for create zk node, try times: " + tryTimes);
            }
            logger.info("Zk node path created: {}", nodePath);
        }
    }

    public LeaderLatch createLeaderLatcher(String masterName, String id) {
        return new LeaderLatch(zkCurator, getMasterHaPath(masterName), id);
    }

    private String getWorkerPath(String dataCenter, String cluster) {
        return String.format("/%s/%s/%s/%s", ZK_RSS_BASE_PATH, dataCenter, cluster, ZK_WORKER_SUBPATH);
    }

    public String getMasterWatchPath(String masterName) {
        return getMasterRootPath(masterName, ServerRole.SS_MASTER);
    }

    public String getMasterHaPath(String masterName) {
        return getMasterRootPath(masterName, ServerRole.SS_MASTER_HA);
    }

    public String getActiveMasterWatchPath() {
        StringBuilder sb = new StringBuilder("/").append(ZK_RSS_BASE_PATH).append("/use_cluster/").append(ZK_MASTER_SUBPATH);
        return sb.toString();
    }

    private String getMasterRootPath(String masterName, ServerRole type) {
        StringBuilder sb = new StringBuilder("/").append(ZK_RSS_BASE_PATH).append("/")
                .append(masterName).append("/").append(ZK_MASTER_SUBPATH).append("/");
        switch (type) {
            case SS_MASTER:
                return sb.append(ZK_MASTER_WATCH_PATH).toString();
            case SS_MASTER_HA:
                return sb.append(ZK_MASTER_HA_PATH).toString();
            default:
                throw new Ors2Exception("Unrecognized zk node type");
        }
    }

    /**
     * Register shuffle worker or master to zk path
     *
     * @param dataCenter
     * @param clusterName
     * @param hostIpPort
     * @param type
     */
    @Override
    public synchronized void registerServer(String dataCenter,
                                            String clusterName,
                                            String hostIpPort,
                                            ServerRole type,
                                            ShuffleWorkerStorageType storageType) {

        if (StringUtils.isBlank(dataCenter)) {
            throw new IllegalArgumentException(String.format("Invalid input: dataCenter=%s", dataCenter));
        }

        if (StringUtils.isBlank(clusterName)) {
            throw new IllegalArgumentException(String.format("Invalid input: clusterName=%s", clusterName));
        }

        if (StringUtils.isBlank(hostIpPort)) {
            throw new IllegalArgumentException(String.format("Invalid input: hostAndPort=%s", hostIpPort));
        }

        final String nodeName = getNodeName(hostIpPort);
        final String nodePath = getNodePath(dataCenter, clusterName, nodeName);

        byte[] nodeDataBytes = new byte[0];
        switch (type) {
            case SS_WORKER:
                final Ors2WorkerDetail workerDetail = new Ors2WorkerDetail(hostIpPort);
                nodeDataBytes = JsonUtils.objToJson(workerDetail).getBytes(StandardCharsets.UTF_8);
                break;
            case SS_MASTER:
            case SS_MASTER_ACTIVE:
            case SS_MASTER_HA:
                // TODO: support register ShuffleMaster roles
                logger.warn("ShuffleMasterRole not support now");
                break;
            case SS_NONE:
            default:
                logger.warn("Invalid ServerRole in registerServer: {}", type);
                break;
        }

        createNode(nodePath, nodeDataBytes, CreateMode.EPHEMERAL);
    }

    private String getNodeName(String hostPort) {
        try {
            return URLEncoder.encode(hostPort, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new Ors2Exception("Failed to get node name for " + hostPort, e);
        }
    }

    private String getNodePath(String dataCenter, String cluster, String address) {
        return getWorkerPath(dataCenter, cluster) + "/" + address;
    }

    public HostPortInfo getMaster(String masterName) {
        if (StringUtils.isBlank(masterName)) {
            throw new IllegalArgumentException(String.format("Invalid input: masterName=%s", masterName));
        }

        String masterInfoPath = getMasterWatchPath(masterName);
        return HostPortInfo.parseFromStr(new String(getZkNodeData(masterInfoPath), StandardCharsets.UTF_8));
    }


    /**
     * Watch the active master
     */
    public NodeCache getNodeCache(String masterName) {
        return new NodeCache(zkCurator, getMasterWatchPath(masterName), false);
    }

    public String getActiveCluster() {
        String activeMasterWatchPath = getActiveMasterWatchPath();
        try {
            if (zkCurator.checkExists().forPath(activeMasterWatchPath) == null) {
                return null;
            }
        } catch (Exception e) {
            logger.error("Error check path: " + activeMasterWatchPath, e);
        }
        byte[] data = getZkNodeData(activeMasterWatchPath);
        if (data == null) {
            return null;
        }
        return new String(data);
    }

    public TreeCache getTreeCache(String path) {
        return new TreeCache(zkCurator, path);
    }

    /**
     * Get shuffle worker id string "hostName:dataPort:connPort"
     *
     * @param path
     * @param node
     * @return
     */
    private String getWorkerZkNodeInfo(String path, String node) {
        String nodePath = new StringBuilder(path).append("/").append(node).toString();
        byte[] bytes = getZkNodeData(nodePath);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Get shuffle servers: workers and master
     *
     * @param dataCenter
     * @param cluster
     * @param count
     * @param role
     * @param storageType
     * @return ["serverIP:port"]
     */
    @Override
    public List<String> getServers(String dataCenter, String cluster, int count, ServerRole role,
                                   ShuffleWorkerStorageType storageType) {
        switch (role) {
            case SS_WORKER:
                return getShuffleWorkers(dataCenter, cluster, count);
            case SS_MASTER:
            case SS_MASTER_HA:
            case SS_MASTER_ACTIVE:
                break;
            case SS_NONE:
            default:
                logger.warn("Invalid server role: {}", role);
                break;
        }
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
        List<String> shuffleWorkers = getShuffleWorkers(dataCenter, cluster, maxCount);
        List<ShuffleMessage.ServerDetail> list = new ArrayList<>(shuffleWorkers.size());
        for (String lem : shuffleWorkers) {
            Ors2WorkerDetail detail = Ors2WorkerDetail.fromJsonString(lem);
            ShuffleMessage.ServerDetail serverDetail = ShuffleMessage.ServerDetail.newBuilder()
                    .setHost(detail.getHost())
                    .setBuildConnPort(detail.getBuildConnPort())
                    .setDataPort(detail.getDataPort())
                    .build();
            list.add(serverDetail);
        }

        return ShuffleMessage.GetWorkersResponse.newBuilder()
                .setDataCenter(dataCenter)
                .setCluster(cluster)
                .addAllSeverDetail(list)
                .build();
    }

    public List<String> getShuffleWorkers(String dataCenter, String clusterName, int maxCount) {
        if (Strings.isNullOrEmpty(dataCenter) || Strings.isNullOrEmpty(clusterName)) {
            String errorInfo = "Illegal args, dc=" + Strings.nullToEmpty(dataCenter)
                    + ", cluster=" + Strings.nullToEmpty(clusterName);
            logger.error(errorInfo);
            throw new Ors2IllegalArgumentException(errorInfo);
        }
        if (maxCount < 1) {
            logger.error("Request shuffle worker count must > 0");
            throw new Ors2IllegalArgumentException("Request shuffle worker count must > 0");
        }

        String path = getWorkerPath(dataCenter, clusterName);
        try {
            List<String> workersHostPort = zkCurator.getChildren().forPath(path);
            if (workersHostPort.size() > maxCount) {
                Collections.shuffle(workersHostPort);
                workersHostPort = workersHostPort.subList(0, maxCount);
            }
            return workersHostPort.stream().map(t -> getWorkerZkNodeInfo(path, t)).collect(Collectors.toList());
        } catch (KeeperException.NoNodeException e) {
            logger.error("ZK path node not exists: " + path);
            return Collections.emptyList();
        } catch (Exception e) {
            throw new Ors2Exception("Unable to retrieve nodes from ZooKeeper", e);
        }
    }

    @Override
    public synchronized void close() {
        if (this.zkCurator == null) {
            logger.warn("ZK curator is null, return");
            return;
        }

        zkCurator.close();
        zkCurator = null;
    }

    public String getZkServerConnStr() {
        return zkServerConnStr;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder("ZkServerConnString:").append(zkServerConnStr).append(", ")
                .append("connTimeoutMs:").append(timeoutMs).append(", ")
                .append("maxRetryTimes:").append(maxRetries).append(", ")
                .append("zkCuratorBaseSleepMs:").append(baseSleepTimeMs).append(", ")
                .append("zkCuratorMaxSleepMs:").append(maxSleepTimeMs);
        return stringBuilder.toString();
    }

    @Override
    public void removeServer(String dataCenter, String cluster, ServerRole role) {

    }

    public List<String> getNodeChildren(String path) {
        try {
            return zkCurator.getChildren().forPath(path);
        } catch (Exception e) {
            logger.error("Error get children for path {}.", path);
        }
        return null;
    }

    public String getZkDagIdPath(String dataCenter, String cluster) {
        return getDagRoot() + String.format("/%s/%s/%s", dataCenter, cluster, ZK_DAG_ID_PATH);
    }

    public String getDagRoot() {
        return String.format("/%s/%s", ZK_RSS_BASE_PATH, ZK_STORAGE_DAG_PATH);
    }

}
