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
import com.oppo.shuttle.rss.messages.MessageConstants;
import com.oppo.shuttle.rss.metrics.Ors2MetricsConstants;
import com.oppo.shuttle.rss.metrics.Ors2MetricsExport;
import com.oppo.shuttle.rss.util.CommonUtils;
import com.oppo.shuttle.rss.common.Pair;
import io.netty.channel.DefaultEventLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Control the shuffle data flow from shuffle sender
 * There are two types control :
 *     1. memory control:
 *        shuffle server memory usage threshold(config): 70%
 *     2. busy control
 *        currently access package count in same time(config): 256 total
 *  Used by {@link BuildConnectionExecutor}
 */
public class FlowController {
  private static final Logger logger = LoggerFactory.getLogger(FlowController.class);
  private final ShuffleServerConfig serverConfig;
  private float memoryControlRatioThreshold;
  private long memoryContolSizeThreshold;
  private int totalConnections;
  private int baseConnections;
  private int barrierConnPerRetry;
  private int barrierConnPerPriorityLevel;
  private AtomicLong usedMemorySize = new AtomicLong();
  private AtomicLong usedMemoryLastUpdateTimeStamp = new AtomicLong();
  private final long CONFIGURED_MAX_JVM_MEMORY = CommonUtils.getJvmConfigMaxMemory();
  private final int MEMORY_UPDATE_INTERVAL_MIL = 15 * 1000;   // 5s
  private final int MAX_RETRY_INDEX = 10;

  // value
  private Map<Integer, ConnectionValue> allConnections = new HashMap<>();
  private final BitSet allocateConnection;
  private volatile int usedConnections = 0;

  // a background executor service doing clean up timeout connections
  private final ScheduledExecutorService clearTimeoutExecutorService = new DefaultEventLoop();

  public FlowController(ShuffleServerConfig serverConfig) {
    this.serverConfig = serverConfig;
    this.memoryContolSizeThreshold = serverConfig.getMemoryControlSizeThreshold();
    this.memoryControlRatioThreshold = serverConfig.getMemoryControlRatioThreshold();
    this.totalConnections = serverConfig.getTotalConnections();
    this.baseConnections = serverConfig.getBaseConnections();
    long usedMemorySize = CommonUtils.getUsedMemory();
    this.usedMemorySize.set(usedMemorySize);
    this.usedMemoryLastUpdateTimeStamp.set(System.currentTimeMillis());
    this.barrierConnPerRetry = serverConfig.getRetryBuildConnectBuffer() / 10; // max retry 10
    this.barrierConnPerPriorityLevel = serverConfig.getPriorityJobBuildConnectBuffer()/ 10; // 10 levels of job priority

    for (int i = 0; i < totalConnections; i++) {
      allConnections.put(i, new ConnectionValue());
    }
    allocateConnection = new BitSet();

    logger.info("InitFreeConnections size: {}", totalConnections);
    clearTimeoutExecutorService.scheduleAtFixedRate(() -> {
      try {
        clearTimeOutConnections();
      } catch (Exception e) {
        logger.warn("Exception occur in clearTimeOutConnections", e);
      }
    }, 60, 60, TimeUnit.SECONDS);

    clearTimeoutExecutorService.scheduleAtFixedRate(() -> {
      try {
        double usedMemory = Ors2MetricsConstants.bufferedDataSize.get();

        if (usedMemory > 1024 || usedConnections > 0) {
          logger.info("flow-monitor, used memory {}MB, remaining memory {} mb; used connection {}, remaining connection {}.",
                  Ors2MetricsExport.toMb(usedMemory),
                  Ors2MetricsExport.toMb(memoryContolSizeThreshold - usedMemory),
                  usedConnections,
                  totalConnections - usedConnections);
        }
      } catch (Exception e) {
        logger.warn("Exception occur in clearTimeOutConnections", e);
      }
    }, MEMORY_UPDATE_INTERVAL_MIL, MEMORY_UPDATE_INTERVAL_MIL, TimeUnit.MILLISECONDS);
  }


  public boolean memoryFlowControlByMetrics() {
    double usedMemory = Ors2MetricsConstants.bufferedDataSize.get();

    if (memoryContolSizeThreshold < usedMemory
            || memoryControlRatioThreshold <= usedMemory / memoryContolSizeThreshold) {
      logger.info("Memory flow control by metrics, usedMemory:{}, memoryContolSizeThreshold: {}, memoryControlRatioThreshold:{}",
              usedMemory, memoryContolSizeThreshold, memoryControlRatioThreshold);
      return true;
    }
    return false;
  }


  public boolean memoryFlowControl() {
    long nowMillSec = System.currentTimeMillis();
    if (nowMillSec - usedMemoryLastUpdateTimeStamp.get() >= MEMORY_UPDATE_INTERVAL_MIL) {
      long usedMemorySize = CommonUtils.getUsedMemory();
      this.usedMemorySize.set(usedMemorySize);
      this.usedMemoryLastUpdateTimeStamp.set(nowMillSec);
    }

    long usedMemory = this.usedMemorySize.get();
    if (CONFIGURED_MAX_JVM_MEMORY < usedMemory
            || memoryControlRatioThreshold <= (double)usedMemory/CONFIGURED_MAX_JVM_MEMORY) {
      logger.info("Memory flow control, usedMemory:{}, CONFIGURED_MAX_JVM_MEMORY: {}, memoryControlRatioThreshold:{}",
              usedMemory, CONFIGURED_MAX_JVM_MEMORY, memoryControlRatioThreshold);
      return true;
    }
    return false;
  }

  public boolean busyFlowControl(int usedConnections, int jobPriority, int retryIdx) {
    retryIdx = Math.min(retryIdx, MAX_RETRY_INDEX);
    if (usedConnections <= baseConnections +
            jobPriority * barrierConnPerPriorityLevel +
            retryIdx * barrierConnPerRetry) {
      return false;
    }
    return true;
  }

  /**
   * @param connIdValue reference of output build-connection Pair<id, value>
   * @param jobPriority high->low (10->1), job access more connection barrier as priority more high,
   *                    default priority is 1
   * @param retryIdx retry index of build-connection request, job access more connection barrier as retryIdx grow
   *                 total buffer for retry is 30 connections
   *                 max retryIdx 10
   * @return Flow control status code
   */
  public int buildConnection(Pair<Integer, Long> connIdValue, int jobPriority, int retryIdx) {
    if (memoryFlowControlByMetrics()) {
      logger.info("MemoryFlowControl jobPriority:{}, retryIdx:{}, runGcImmediately!", jobPriority, retryIdx);
      //SystemUtils.runGcImmediately();
      Ors2MetricsConstants.memoryControlTimes.inc();
      return MessageConstants.FLOW_CONTROL_MEMORY;
    }

    try {
      synchronized (allocateConnection) {

        if (busyFlowControl(usedConnections, jobPriority, retryIdx)) {
          logger.info("BusyFlowControl jobPriority:{}, retryIdx:{}, usedConnections:{}, baseConnections: {}, freeConnections: {}" +
                          " barrierConnPerPriorityLevel:{}",
                  jobPriority, retryIdx, allocateConnection.cardinality(), baseConnections, totalConnections - usedConnections, barrierConnPerPriorityLevel);
          Ors2MetricsConstants.busyControlTimes.inc();
          return MessageConstants.FLOW_CONTROL_BUSY;
        }

        int id = allocateConnection.nextClearBit(0);
        if (id >= totalConnections) {
          Ors2MetricsConstants.busyControlTimes.inc();
          return MessageConstants.FLOW_CONTROL_BUSY;
        }

        allocateConnection.set(id);
        ConnectionValue value = allConnections.get(id);
        value.update();

        connIdValue.setKey(id);
        connIdValue.setValue(value.getConnectionValue());

        usedConnections += 1;
      }

      Ors2MetricsConstants.connTotalCount.inc();
      return MessageConstants.FLOW_CONTROL_NONE;
    } catch (Throwable e) {
      logger.error("Failed to allocate connection", e);
      Ors2MetricsConstants.busyControlTimes.inc();
      return MessageConstants.FLOW_CONTROL_BUSY;
    }
  }

  public void releaseConnection(int id) {
    if (id < 0 || !allConnections.containsKey(id)) {
      logger.warn("releaseConnection invalid id: {}", id);
    }

    synchronized (allocateConnection) {
      allocateConnection.clear(id);
      allConnections.get(id).reset();
      usedConnections -= 1;
    }
    Ors2MetricsConstants.connReleasedCount.inc();
  }

  public void clearTimeOutConnections() {
    final long nowMilSec = System.currentTimeMillis();
    final long timeoutInterval = serverConfig.getFlowControlBuildIdTimeout();
    List<Map.Entry<Integer, ConnectionValue>> toReleaseConn = allConnections.entrySet().stream()
            .filter(en -> en.getValue().expired(nowMilSec, timeoutInterval))
            .collect(Collectors.toList());

    if (toReleaseConn.size() == 0) {
      return;
    }

    synchronized (allocateConnection) {
      toReleaseConn.forEach(entry -> {
        if (entry.getValue().expired(nowMilSec, timeoutInterval)) {
          allocateConnection.clear(entry.getKey());
          allConnections.get(entry.getKey()).reset();
        }
      });
      usedConnections -= toReleaseConn.size();
    }

    logger.info("Release timeout connection, totalSize: {}", toReleaseConn.size());
  }

  public boolean checkIdValue(int id, long value) {
    ConnectionValue connection = allConnections.get(id);
    if (connection == null) {
      return false;
    } else {
      return connection.getConnectionValue() == value;
    }
  }
}