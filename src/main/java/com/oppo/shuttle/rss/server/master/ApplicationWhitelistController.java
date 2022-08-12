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

import com.oppo.shuttle.rss.messages.ShuffleMessage;
import com.oppo.shuttle.rss.metadata.ZkShuffleServiceManager;
import com.oppo.shuttle.rss.util.JsonUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.Set;

public class ApplicationWhitelistController {
    private static final Logger logger = LoggerFactory.getLogger(ApplicationWhitelistController.class);

    private boolean enable;

    private final ZkShuffleServiceManager zk;

    private WhitelistBean whiteList;

    private NodeCache nodeCache;

    public static class WhitelistBean {
        private LinkedHashSet<String> dagIdList = new LinkedHashSet<>();
        private LinkedHashSet<String> taskIdList = new LinkedHashSet<>();
        private LinkedHashSet<String> appNameList = new LinkedHashSet<>();

        public void setDagIdList(LinkedHashSet<String> dagIdList) {
            this.dagIdList = dagIdList;
        }

        public void setTaskIdList(LinkedHashSet<String> taskIdList) {
            this.taskIdList = taskIdList;
        }

        public void setAppNameList(LinkedHashSet<String> appNameList) {
            this.appNameList = appNameList;
        }

        public LinkedHashSet<String> getDagIdList() {
            return dagIdList;
        }

        public LinkedHashSet<String> getTaskIdList() {
            return taskIdList;
        }

        public LinkedHashSet<String> getAppNameList() {
            return appNameList;
        }
    }

    public ApplicationWhitelistController(ZkShuffleServiceManager zk, boolean enable) {
        this.enable = enable;
        this.zk = zk;
        init();
    }

    private void init() {
        try {
            nodeCache = zk.createNodeCache(zk.getWhitelistRoot());
            nodeCache.start();
            nodeCache.getListenable().addListener(this::reloadConf);
        } catch (Exception e) {
            enable = false;
            logger.error("zk monitoring failed, the whitelist check is turned off", e);
        }
    }

    private synchronized void reloadConf() {
        try {
            whiteList = getZkWhiteList();
            logger.info("Whitelist loaded successfully, dagSize: {}, taskSize: {}, appSize: {}",
                    whiteList.dagIdList.size(), whiteList.taskIdList.size(), whiteList.appNameList.size());
        } catch (Exception e) {
            logger.warn("Failed to load whitelist, validation will be disabled.", e);
            enable = false;
        }
    }

    public synchronized boolean checkIsWriteList(ShuffleMessage.GetWorkersRequest request) {
        String dagId = request.getDagId();
        String taskId = request.getTaskId();
        String appName = request.getAppName();
        String baseMsg = String.format("dagId=%s, taskId=%s, appId=%s, appName=%s",
                dagId, taskId, request.getAppId(), appName);

        if (!enable || whiteList == null) {
            logger.info("Whitelist check success(pass) for {}", baseMsg);
            return true;
        }

        // If the dag id check passes, return directly.
        if (StringUtils.isNotEmpty(dagId) && whiteList.dagIdList.contains(dagId)) {
            logger.info("Whitelist check success(dagId) for {}", baseMsg);
            return true;
        }

        // If taskId exists, check taskId, otherwise check appName。
        boolean checkSuccess = false;
        if (StringUtils.isNotEmpty(taskId)) {
            checkSuccess = whiteList.taskIdList.contains(dagId + "." + taskId);
            if (checkSuccess) {
                logger.info("Whitelist check success(taskId) for {}", baseMsg);
            }
        } else {
            for (String prefix : whiteList.appNameList) {
                if (StringUtils.startsWithIgnoreCase(appName, prefix)) {
                    checkSuccess = true;
                    logger.info("Whitelist check success(appName) for {}", baseMsg);
                    break;
                }
            }
        }

        return checkSuccess;
    }

    private WhitelistBean getZkWhiteList() {
        byte[] data = nodeCache.getCurrentData().getData();
        whiteList = JsonUtils.jsonToObj(new String(data), WhitelistBean.class);
        return whiteList;
    }

    private void add(Set<String> requestSet, Set<String> zkSet, String mark) {
        requestSet.forEach(value -> {
            if (zkSet.add(value)) {
                logger.info("whitelist-op-add: {}({}) added whitelist successfully", mark, value);
            } else {
                logger.info("whitelist-op-add: {}({}) already on the whitelist", mark, value);
            }
        });
    }

    /**
     * {
     *     "dag": "task.id",
     *     "tasks": "t1,t2"
     * }
     */
    public synchronized void add(String json) {
        logger.info("whitelist-op-add: request {}", json);

        WhitelistBean requestObj = JsonUtils.jsonToObj(json, WhitelistBean.class);
        WhitelistBean zkObj = getZkWhiteList();
        add(requestObj.dagIdList, zkObj.dagIdList, "dag");
        add(requestObj.taskIdList, zkObj.taskIdList, "task");
        add(requestObj.appNameList, zkObj.appNameList, "name");

        String res = JsonUtils.objToJson(zkObj);
        zk.setData(zk.getWhitelistRoot(), res.getBytes());
    }

    private void remove(Set<String> requestSet, Set<String> zkSet, String mark) {
        requestSet.forEach(value -> {
            if (zkSet.remove(value)) {
                logger.info("whitelist-op-remove: {}({}) remove whitelist successfully", mark, value);
            } else {
                logger.info("whitelist-op-remove: {}({}) not on the whitelist", mark, value);
            }
        });
    }

    public synchronized void remove(String json) {
        logger.info("whitelist-op-remove: request {}", json);

        WhitelistBean requestObj = JsonUtils.jsonToObj(json, WhitelistBean.class);
        WhitelistBean zkObj = getZkWhiteList();

        remove(requestObj.dagIdList, zkObj.dagIdList, "dag");
        remove(requestObj.taskIdList, zkObj.taskIdList, "task");
        remove(requestObj.appNameList, zkObj.appNameList, "name");

        String res = JsonUtils.objToJson(zkObj);
        zk.setData(zk.getWhitelistRoot(), res.getBytes());
    }

    public synchronized WhitelistBean getWhiteList() {
        return whiteList;
    }
}
