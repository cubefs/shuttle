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

public class AppTaskInfo {
  private final String appId;
  private final String appAttempt;
  private final int shuffleWorkerSize;
  private final int numPartitionBlock;
  private final int shuffleId;
  private final int mapId;
  private final long taskAttemptId;
  private final int stageAttempt;

  public AppTaskInfo(String appId, String appAttempt, int shuffleWorkerSize,
      int numPartitionBlock, int shuffleId, int mapId, long taskAttemptId, int stageAttempt) {
    this.appId = appId;
    this.appAttempt = appAttempt;
    this.shuffleWorkerSize = shuffleWorkerSize;
    this.numPartitionBlock = numPartitionBlock;
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.taskAttemptId = taskAttemptId;
    this.stageAttempt = stageAttempt;
  }

  public int getMapId() {
    return mapId;
  }

  public int getNumPartitionBlock() {
    return numPartitionBlock;
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public String getAppAttempt() {
    return appAttempt;
  }

  public int getShuffleWorkerSize() {
    return shuffleWorkerSize;
  }

  public long getTaskAttemptId() {
    return taskAttemptId;
  }

  public int getStageAttempt() {
    return stageAttempt;
  }

  @Override
  public String toString() {
    return "AppTaskInfo{" +
        "appId='" + appId + '\'' +
        ", appAttempt='" + appAttempt + '\'' +
        ", shuffleWorkerSize=" + shuffleWorkerSize +
        ", numPartitionBlock=" + numPartitionBlock +
        ", shuffleId=" + shuffleId +
        ", mapId=" + mapId +
        ", taskAttemptId=" + taskAttemptId +
        '}';
  }
}
