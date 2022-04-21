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

package com.oppo.shuttle.rss.execution;

import com.oppo.shuttle.rss.ShuffleServerConfig;
import com.oppo.shuttle.rss.messages.ShuffleMessage;
import com.oppo.shuttle.rss.common.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process ors2 shuffle sender build connection request
 */
public class BuildConnectionExecutor {
  private static final Logger logger = LoggerFactory.getLogger(BuildConnectionExecutor.class);
  private FlowController flowController;

  public BuildConnectionExecutor(ShuffleServerConfig serverConfig) {
    logger.info("Start BuildConnectionExecutor");
    this.flowController = new FlowController(serverConfig);
  }

  public int buildConnection(ShuffleMessage.BuildConnectionRequest connectionRequest, Pair<Integer, Long> result) {
    return flowController.buildConnection(result, connectionRequest.getJobPriority(), connectionRequest.getRetryIdx());
  }

  public boolean checkConnectionIdValue(int id, long value) {
    return flowController.checkIdValue(id, value);
  }

  public void releaseConnection(int id) {
    flowController.releaseConnection(id);
  }

}
