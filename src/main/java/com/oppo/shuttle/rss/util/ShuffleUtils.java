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

package com.oppo.shuttle.rss.util;

import com.oppo.shuttle.rss.common.ConfiguredServerList;
import com.oppo.shuttle.rss.common.Ors2WorkerDetail;
import com.oppo.shuttle.rss.exceptions.Ors2Exception;
import com.oppo.shuttle.rss.exceptions.Ors2RandomMockException;
import com.oppo.shuttle.rss.messages.ShuffleMessage;
import com.oppo.shuttle.rss.metadata.ServiceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.stream.Collectors;

public class ShuffleUtils {
  private static final Logger logger = LoggerFactory.getLogger(ShuffleUtils.class);
  private static Random random = new Random();

  public static void rollQueues(Queue from, Queue to) {
    if (from == null || to == null) {
      return;
    }

    while (from.peek() != null) {
      to.offer(from.poll());
    }
  }

  public static void randomErrorInjection(double probability) {
    if (probability > 0) {
      random.setSeed(System.nanoTime());
      if (probability > random.nextDouble()) {
        logger.info("TriggerRandomMockException");
        throw new Ors2RandomMockException("Random mock exception for testing");
      }
    }
  }

  public static int generateShuffleExecutorIndex(String appId, long shuffleId, int partitionId, int executorCount) {
    return (appId.hashCode() + (int)shuffleId + partitionId) % executorCount;
  }

  /***
   * Get all servers from service registry with retry
   * @param serviceManager service manager instance
   * @param maxServerCount max server count to return
   * @param maxTryMillis max trying milliseconds
   * @param dataCenter data center
   * @param cluster cluster
   * @return servers
   */
  public static ConfiguredServerList getShuffleServersWithoutCheck(
          ServiceManager serviceManager,
          int maxServerCount,
          long maxTryMillis,
          String dataCenter,
          String cluster,
          String appId,
          String dagId,
          int jobPriority,
          String taskId,
          String appName) {

    int retryIntervalMillis = 15000;
    ShuffleMessage.GetWorkersResponse response = CommonUtils.retry(
            retryIntervalMillis,
            6 * retryIntervalMillis,
            () -> {
              try {
                logger.info("Trying to get max {} shuttle rss workers, data center: {}, cluster: {}, " ,
                        maxServerCount, dataCenter, cluster);
                return serviceManager.getServersWithConf(dataCenter, cluster, maxServerCount,
                        jobPriority, appId, dagId, taskId, appName);
              } catch (Throwable ex) {
                logger.warn("Failed to call ServiceRegistry.getServers", ex);
                return null;
              }
            });

    if (response == null || response.getSeverDetailList().isEmpty()) {
      throw new Ors2Exception("Failed to get all ORS2 servers");
    }
    List<Ors2WorkerDetail> severDetailList = response.getSeverDetailList().stream()
            .map(Ors2WorkerDetail::convertFromProto)
            .collect(Collectors.toList());

    return new ConfiguredServerList(severDetailList,
            response.getFileSystemConf(),
            response.getRootDir(),
            response.getDataCenter(),
            response.getCluster());
  }

}
