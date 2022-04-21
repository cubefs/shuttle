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


import java.util.Objects;

/**
 * App/Stage/Partition shuffle info obj
 */
public class PartitionShuffleId {
    private final StageShuffleId stageShuffleId;
    private final int partitionId;

    public PartitionShuffleId(StageShuffleId stageShuffleId, int partitionId) {
        this.stageShuffleId = stageShuffleId;
        this.partitionId = partitionId;
    }

    public PartitionShuffleId(String appId, String appAttempt, int stageAttempt, int shuffleId, int partitionId) {
        stageShuffleId = new StageShuffleId(appId, appAttempt, stageAttempt, shuffleId);
        this.partitionId = partitionId;
    }

    public StageShuffleId getStageShuffleId() {
        return stageShuffleId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionShuffleId that = (PartitionShuffleId) o;
        return partitionId == that.partitionId &&
                Objects.equals(stageShuffleId, that.stageShuffleId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stageShuffleId, partitionId);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("PartitionShuffleId{ stageShuffleId=").append(stageShuffleId.toString())
                .append(", partitionId=").append(partitionId).append("}");
        return sb.toString();
    }
}
