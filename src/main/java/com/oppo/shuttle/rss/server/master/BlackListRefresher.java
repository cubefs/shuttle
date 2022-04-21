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

import com.oppo.shuttle.rss.util.ConfUtil;
import com.oppo.shuttle.rss.util.ScheduledThreadPoolUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BlackListRefresher {

    private static final Logger logger = LoggerFactory.getLogger(BlackListRefresher.class);

    private List<String> workerBlackList;
    private final long updateDelay;
    private final long blackListRefreshInterval;

    public BlackListRefresher(
            long updateDelay,
            long blackListRefreshInterval) {
        this.updateDelay = updateDelay;
        this.blackListRefreshInterval = blackListRefreshInterval;
        refreshStart();
    }

    public void refreshStart(){
        ScheduledThreadPoolUtils.scheduleAtFixedRate(
                this::refreshBlackList,
                updateDelay,
                blackListRefreshInterval);
    }

    private void refreshBlackList(){
        workerBlackList = ConfUtil.getWorkerBlackList();
        logger.info("Refresh {} workers to black list from config", workerBlackList.size());
        workerBlackList.forEach(ShuffleWorkerStatusManager::setToBlackStatus);
    }

    public List<String> getWorkerBlackList() {
        return workerBlackList;
    }
}
