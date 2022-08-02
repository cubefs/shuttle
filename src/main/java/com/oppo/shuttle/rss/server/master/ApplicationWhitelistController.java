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
import com.oppo.shuttle.rss.util.ConfUtil;
import com.oppo.shuttle.rss.util.JsonUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class ApplicationWhitelistController {
    private static final Logger logger = LoggerFactory.getLogger(ApplicationWhitelistController.class);

    public final String confPath;

    private boolean enable;

    private long lastReadTime = 0;

    private long lastModifyTime = 0;

    private final static long READ_INTERVAL = TimeUnit.MINUTES.toMillis(1);

    private WhiteList whiteList;

    public static class WhiteList{
        private HashSet<String> dagIdList;
        private HashSet<String> taskIdList;
        private HashSet<String> appNameList;

        public HashSet<String> getTaskIdList() {
            return taskIdList;
        }

        public void setTaskIdList(HashSet<String> taskIdList) {
            this.taskIdList = taskIdList;
        }

        public HashSet<String> getAppNameList() {
            return appNameList;
        }

        public void setAppNameList(HashSet<String> appNameList) {
            this.appNameList = appNameList;
        }

        public HashSet<String> getDagIdList() {
            return dagIdList;
        }

        public void setDagIdList(HashSet<String> dagIdList) {
            this.dagIdList = dagIdList;
        }
    }

    public ApplicationWhitelistController(boolean enable) {
        this.confPath = ConfUtil.getRSSConfDir() + "/application-whitelist.json";
        this.enable = enable;
        readConf();
    }

    private synchronized void readConf() {
        long now = System.currentTimeMillis();
        if (now - lastReadTime > READ_INTERVAL) {
            long modified = new File(confPath).lastModified();
            if (modified > lastModifyTime) {
                try {
                    String json = FileUtils.readFileToString(new File(confPath), StandardCharsets.UTF_8);
                    whiteList = JsonUtils.jsonToObj(json, WhiteList.class);
                    logger.info("Whitelist loaded successfully, dagSize: {}, taskSize: {}, appSize: {}",
                            whiteList.dagIdList.size(), whiteList.taskIdList.size(), whiteList.appNameList.size());
                } catch (Throwable e) {
                    logger.warn("Failed to load whitelist, validation will be disabled.", e);
                    enable = false;
                } finally {
                    lastReadTime = now;
                    lastModifyTime = modified;
                }
            }
        }
    }

    public synchronized boolean checkIsWriteList(ShuffleMessage.GetWorkersRequest request) {
        readConf();

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
            checkSuccess = whiteList.taskIdList.contains(taskId);
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
}
