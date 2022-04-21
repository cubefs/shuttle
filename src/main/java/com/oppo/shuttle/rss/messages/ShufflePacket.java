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
import io.netty.buffer.PooledByteBufAllocator;
import java.util.List;

/**
 * Network sending request message encapsulation
 */
public class ShufflePacket {
    private final ShuffleMessage.BuildConnectionRequest.Builder buildBuilder;
    private final ShuffleMessage.UploadPackageRequest.Builder dataBuilder;

    private final List<byte[]> partitionBlocks;

    private final String id;

    private int dataSize;

    public static int HEADER_SIZE = 12;

    public ShufflePacket(
            ShuffleMessage.BuildConnectionRequest.Builder buildBuilder,
            ShuffleMessage.UploadPackageRequest.Builder dataBuilder,
            List<byte[]> partitionBlocks) {
        this.buildBuilder = buildBuilder;
        this.dataBuilder = dataBuilder;
        this.partitionBlocks = partitionBlocks;

        String id1 = buildBuilder.getMessageId();
        String id2 = dataBuilder.getMessageId();
        if (!id1.equals(id2)) {
            throw new RuntimeException("buildBuilder and dataBuilder message id not equal");
        }
        partitionBlocks.forEach(data -> dataSize += data.length);

        this.id = id1;
    }

    public ShuffleMessage.BuildConnectionRequest.Builder getBuildBuilder() {
        return buildBuilder;
    }

    public ShuffleMessage.UploadPackageRequest.Builder getDataBuilder() {
        return dataBuilder;
    }

    public List<byte[]> getPartitionBlocks() {
        return partitionBlocks;
    }

    public String getId() {
        return id;
    }

    public int getDataSize() {
        return dataSize;
    }

    private static ByteBuf createBuf(int size) {
        return PooledByteBufAllocator.DEFAULT.buffer(size);
    }

    public ByteBuf buildBuffer() {
        buildBuilder.setSendTime(System.currentTimeMillis());
        ShuffleMessage.BuildConnectionRequest request = buildBuilder.build();

        int serializedSize = request.getSerializedSize();
        ByteBuf buf = createBuf(HEADER_SIZE + serializedSize);

        // protocol
        buf.writeInt(MessageConstants.MESSAGE_BuildConnectionRequest);
        buf.writeInt(Integer.BYTES + serializedSize);

        // body
        buf.writeInt(serializedSize);
        buf.writeBytes(request.toByteArray());

        return buf;
    }

    public ByteBuf dataBuffer(int id, long value) {
        dataBuilder.setSendTime(System.currentTimeMillis());
        dataBuilder.setBuildId(id);
        dataBuilder.setBuildValue(value);

        ShuffleMessage.UploadPackageRequest request = dataBuilder.build();
        int serializedSize = request.getSerializedSize();
        ByteBuf buf = createBuf(HEADER_SIZE + serializedSize + dataSize);

        buf.writeInt(MessageConstants.MESSAGE_UploadPackageRequest); // msg type
        buf.writeInt(Integer.BYTES + serializedSize + dataSize); // total length

        buf.writeInt(serializedSize); // header size
        buf.writeBytes(request.toByteArray()); // body
        partitionBlocks.forEach(buf::writeBytes);

        return buf;
    }

    public static ShufflePacket create(
            ShuffleMessage.BuildConnectionRequest.Builder buildBuilder,
            ShuffleMessage.UploadPackageRequest.Builder dataBuilder,
            List<byte[]> shuffleData) {
        return new ShufflePacket(buildBuilder, dataBuilder, shuffleData);
    }
}
