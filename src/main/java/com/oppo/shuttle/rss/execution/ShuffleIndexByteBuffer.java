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

public class ShuffleIndexByteBuffer {
    private byte[] bytes;
    private int capacity;
    private int index;

    public ShuffleIndexByteBuffer(int capacity) {
        bytes = new byte[capacity];
        this.capacity = capacity;
        index = 0;
    }

    public void clear() {
        this.index = 0;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public void incrementIndex(int delta) {
        index += delta;
    }

    public void writeInt(int x) throws Exception {
        if (index + Integer.BYTES > capacity) {
            throw new Exception("ShuffleIndexByteBuffer writeInt not enough space");
        }
        bytes[index] = (byte)(x >> 24);
        bytes[index + 1] = (byte)(x >> 16);
        bytes[index + 2] = (byte)(x >> 8);
        bytes[index + 3] = (byte)(x);
        index += Integer.BYTES;
    }

    public void writeLong(long x) throws Exception {
        if (index + Long.BYTES > capacity) {
            throw new Exception("ShuffleIndexByteBuffer writeInt not enough space");
        }
        bytes[index] = (byte)((int)(x >> 56));
        bytes[index + 1] = (byte)((int)(x >> 48));
        bytes[index + 2] = (byte)((int)(x >> 40));
        bytes[index + 3] = (byte)((int)(x >> 32));
        bytes[index + 4] = (byte)((int)(x >> 24));
        bytes[index + 5] = (byte)((int)(x >> 16));
        bytes[index + 6] = (byte)((int)(x >> 8));
        bytes[index + 7] = (byte)((int)(x));
        index += Long.BYTES;
    }
}
