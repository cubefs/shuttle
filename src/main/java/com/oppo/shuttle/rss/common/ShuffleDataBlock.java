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

import java.util.Arrays;
import java.util.Objects;

public class ShuffleDataBlock {
    private final byte[] data;
    private int length;
    private int taskId;
    private long taskAttemptId;
    private int seqId;

    public ShuffleDataBlock(byte[] data, int length, int taskId, long taskAttemptId, int seqId) {
        this.data = data;
        this.length = length;
        this.taskId = taskId;
        this.taskAttemptId = taskAttemptId;
        this.seqId = seqId;
    }

    public byte[] getData() {
        return data;
    }

    public int getLength() {
        return length;
    }

    public int getTaskId() {
        return taskId;
    }

    public long getTaskAttemptId() {
        return taskAttemptId;
    }

    public int getSeqId() {
        return seqId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShuffleDataBlock that = (ShuffleDataBlock) o;
        return length == that.length &&
                taskId == that.taskId &&
                taskAttemptId == that.taskAttemptId &&
                seqId == that.seqId &&
                Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(length, taskId, taskAttemptId, seqId);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }
}
