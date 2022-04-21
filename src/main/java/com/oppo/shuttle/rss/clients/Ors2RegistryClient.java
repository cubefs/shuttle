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

package com.oppo.shuttle.rss.clients;

import com.oppo.shuttle.rss.messages.MessageConstants;
import com.oppo.shuttle.rss.messages.ShuffleMessage;
import com.oppo.shuttle.rss.messages.ShuffleMessage.ShuffleWorkerHealthInfo;
import com.oppo.shuttle.rss.metadata.ZkShuffleServiceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Ors2RegistryClient implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(Ors2RegistryClient.class);

    private final Ors2MasterClient ors2MasterClient;

    public Ors2RegistryClient(
            ZkShuffleServiceManager zkManager,
            long timeoutMillis,
            long retryInterval,
            String masterName,
            boolean useEpoll) {
        ors2MasterClient = new Ors2MasterClient(zkManager, timeoutMillis, retryInterval, masterName, useEpoll);
    }

    public void sendHeartbeat(ShuffleWorkerHealthInfo shuffleWorkerHealthInfo) {
        logger.debug("Worker connecting to master for heartbeat: {}", ors2MasterClient);

        ors2MasterClient.writeMsg(shuffleWorkerHealthInfo, MessageConstants.MESSAGE_SHUFFLE_WORKER_HEALTH_INFO);
    }

    public void unregisterFromMaster(ShuffleMessage.ShuffleWorkerUnregisterRequest shuffleWorkerUnregisterRequest) {
        logger.debug("Worker disconnect from master.");

        ors2MasterClient.writeMsg(
                shuffleWorkerUnregisterRequest,
                MessageConstants.MESSAGE_SHUFFLE_WORKER_UNREGISTER_REQUEST);

    }

    @Override
    public void close() {
        ors2MasterClient.close();
    }
}
