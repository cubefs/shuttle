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

import java.util.List;

public class WeightedRandomDispatcher extends ShuffleMasterDispatcher {

    public WeightedRandomDispatcher(ShuffleServerConfig masterConfig) {
        super(masterConfig);
    }

    @Override
    public MasterDispatchServers getServerList(
            int requestCount,
            String dataCenter,
            String cluster,
            String dagId,
            int jobPriority) {
        MasterDispatchServers masterDispatchServers = getAvailableServers(dataCenter, cluster, dagId);
        List<Ors2WorkerDetail> serverList = masterDispatchServers.getCandidatesByWeight(requestCount);

        if (serverList.isEmpty()) {
            return masterDispatchServers;
        }

        masterDispatchServers.setServerDetailList(serverList);
        return masterDispatchServers;
    }
}
