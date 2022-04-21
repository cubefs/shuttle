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

package com.oppo.shuttle.rss.messages;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public class ShuffleData {
    private final ShuffleMessage.UploadPackageRequest uploadPackage;
    private final List<Block> partitionBlocks;

    public static class Block {
        private final int partition;
        private final int seqId;
        private final byte[] data;

        public Block(int partition, int seqId, byte[] data) {
            this.partition = partition;
            this.seqId = seqId;
            this.data = data;
        }

        public int getPartition() {
            return partition;
        }

        public int getSeqId() {
            return seqId;
        }

        public byte[] getData() {
            return data;
        }
    }

    public ShuffleData(ShuffleMessage.UploadPackageRequest uploadPackage, List<Block> partitionBlocks) {
        this.uploadPackage = uploadPackage;
        this.partitionBlocks = partitionBlocks;
    }

    public ShuffleMessage.UploadPackageRequest getUploadPackage() {
        return uploadPackage;
    }

    public List<Block> getPartitionBlocks() {
        return partitionBlocks;
    }

    public static ShuffleData parseFrom(ShuffleMessage.UploadPackageRequest uploadPackage, ByteBuf buf) {
        ArrayList<Block> partitionBlocks = new ArrayList<>(uploadPackage.getPartitionBlocksCount());

        uploadPackage.getPartitionBlocksList().forEach(blockData -> {
            int len = blockData.getDataLength();
            byte[] bytes = new byte[len];
            buf.readBytes(bytes);
            partitionBlocks.add(new Block(blockData.getPartitionId(), blockData.getSeqId(), bytes));
        });

        return new ShuffleData(uploadPackage, partitionBlocks);
    }
}
