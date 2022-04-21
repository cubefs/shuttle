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

import com.oppo.shuttle.rss.exceptions.Ors2Exception;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Ors2ServerSwitchGroup {
    private static final Logger logger = LoggerFactory.getLogger(Ors2ServerSwitchGroup.class);

    private final Map<Integer, ServerLocation> groupMap = new HashMap<>();

    private final int workerRetryNum;

    private final int mapId;

    private final boolean mapWriteDispersion;

    public static class ServerLocation {
        private final List<Ors2WorkerDetail> availableList;
        private final Set<Ors2WorkerDetail> blackSet;
        public int workerId;

        public ServerLocation(int workerId, List<Ors2WorkerDetail> serverList) {
            this.workerId = workerId;
            this.availableList = new ArrayList<>(serverList);
            this.blackSet = new LinkedHashSet<>();
        }

        public synchronized void removeServer(Ors2WorkerDetail server) {
            if (blackSet.contains(server)) {
                return;
            }

            availableList.remove(server);
            blackSet.add(server);
            logger.warn("Remove unavailable server {}", server);
            if (availableList.size() == 0) {
                logger.warn("If there is no server available, add all the blacklisted servers to the available list.");
                availableList.addAll(blackSet);
                blackSet.clear();
            }
        }

        public int availableSize() {
            return availableList.size();
        }

        public Ors2WorkerDetail getServer(int index) {
            return availableList.get(index);
        }
    }


    public Ors2ServerSwitchGroup(List<Ors2ServerGroup> groupList, int mapId, int workerRetryNum, boolean mapWriteDispersion) {
        for (int i = 0; i < groupList.size(); i++) {
            groupMap.put(i, new ServerLocation(i, groupList.get(i).getServers()));
        }

        this.workerRetryNum = workerRetryNum;
        this.mapId = mapId;
        this.mapWriteDispersion = mapWriteDispersion;
    }

    public Ors2WorkerDetail getServer(int workerId, int retry, Optional<Ors2WorkerDetail> serverDetail) {
        ServerLocation location = groupMap.get(workerId);
        if (location == null) {
            throw new Ors2Exception("Invalid server group id: " + workerId);
        }

        if (serverDetail.isPresent()) {
            location.removeServer(serverDetail.get());
            retry = 0;
        }

        int index;
        if (mapWriteDispersion) {
            index = mapId % location.availableSize();
        } else {
            index = (retry / workerRetryNum) % location.availableSize();
        }

        return location.getServer(index);
    }

    public int availableSize(int workerId) {
        return groupMap.get(workerId).availableSize();
    }

    public void  removeServer(int workerId, Ors2WorkerDetail server) {
        groupMap.get(workerId).removeServer(server);
    }
}
