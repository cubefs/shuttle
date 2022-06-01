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

import com.oppo.shuttle.rss.common.Constants;
import com.oppo.shuttle.rss.decoders.Ors2MasterClientDecoder;
import com.oppo.shuttle.rss.messages.MessageConstants;
import com.oppo.shuttle.rss.messages.ShuffleMessage;
import com.oppo.shuttle.rss.metadata.ZkShuffleServiceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Ors2GetWorkersClient implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(Ors2GetWorkersClient.class);

    private final Ors2MasterClient ors2MasterClient;

    private final Ors2MasterClientDecoder ors2MasterClientDecoder;

    public Ors2GetWorkersClient(ZkShuffleServiceManager zkShuffleServiceManager,
                                long timeoutMillis,
                                long retryInterval,
                                String masterName,
                                boolean useEpoll) {
        ors2MasterClient = new Ors2MasterClient(zkShuffleServiceManager, timeoutMillis, retryInterval, masterName, useEpoll);
        ors2MasterClientDecoder = ors2MasterClient.getWorkerClientRegistryDecoder();
    }


    /**
     * Get shuffle workers from shuffle master
     * @param getWorkersRequest
     * @return list of shuffle worker info [string(ip:dataPort:connPort)]
     */
    public synchronized ShuffleMessage.GetWorkersResponse getServers(ShuffleMessage.GetWorkersRequest getWorkersRequest) {
        try {
            CountDownLatch latch = new CountDownLatch(1);
            ors2MasterClient.writeMsg(getWorkersRequest, MessageConstants.MESSAGE_DRIVER_REQUEST_INFO);
            ors2MasterClientDecoder.resetLatch(latch);
            if (!latch.await(Constants.SHUFFLE_GET_RESULT_TIMEOUT, TimeUnit.SECONDS)){
                throw new TimeoutException("Waiting too long for shuffle master response.");
            }
            return (ShuffleMessage.GetWorkersResponse) ors2MasterClientDecoder.getResult();
        } catch (InterruptedException | TimeoutException t) {
            logger.warn("Can't get any workers in {} seconds, try again later. ", Constants.SHUFFLE_GET_RESULT_TIMEOUT, t);
            return null;
        }
    }

    @Override
    public void close() {
        ors2MasterClient.close();
    }
}
