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

import com.oppo.shuttle.rss.common.Constants;
import com.oppo.shuttle.rss.messages.SerializableMessage;
import com.oppo.shuttle.rss.storage.fs.FSDataInputStream;
import com.oppo.shuttle.rss.util.ByteUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;

/**
 * Index info to flush to index file
 */
public class ShuffleIndex extends SerializableMessage {
  public static int NUM_BYTES = Long.BYTES * 3 + Integer.BYTES * 2;
  private int mapId;
  private long attemptId;
  private int seqId;
  private long offset;
  private long length;

  public ShuffleIndex(int mapId, long attemptId, int seqId) {
    this.mapId = mapId;
    this.attemptId = attemptId;
    this.seqId = seqId;
    this.offset = Long.MIN_VALUE;
    this.length = 0;
  }

  public ShuffleIndex(int mapId, long attemptId, int seqId, long offset, long length) {
    this.mapId = mapId;
    this.attemptId = attemptId;
    this.seqId = seqId;
    this.offset = offset;
    this.length = length;
  }

  public int getMapId() {
    return mapId;
  }

  public void setMapId(int mapId) {
    this.mapId = mapId;
  }

  public long getAttemptId() {
    return attemptId;
  }

  public void setAttemptId(long attemptId) {
    this.attemptId = attemptId;
  }

  public int getSeqId() {
    return seqId;
  }

  public void setSeqId(int seqId) {
    this.seqId = seqId;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public long getLength() {
    return length;
  }

  public void setLength(long length) {
    this.length = length;
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(mapId);
    buf.writeLong(attemptId);
    buf.writeInt(seqId);
    buf.writeLong(offset);
    buf.writeLong(length);
  }

  public byte[] serialize() {
    ByteBuf buf = Unpooled.buffer(NUM_BYTES);
    serialize(buf);
    return buf.array();
  }

  @Override
  public String toString() {
    return "ShuffleIndex: " + mapId + "\t" + attemptId + "\t"
      + seqId + "\t" + offset + "\t" + length + "\n";
  }

  public static ShuffleIndex deserialize(ByteBuf buf) {
    int mId = buf.readInt();
    long aId = buf.readLong();
    int sId = buf.readInt();
    long off = buf.readLong();
    long len = buf.readLong();
    return new ShuffleIndex(mId, aId, sId, off, len);
  }

  public static ShuffleIndex deserializeFromBytes(byte[] bytes) {
    int mId = ByteUtil.readInt(bytes, 0);
    long aId = ByteUtil.readLong(bytes, Integer.BYTES);
    int sId = ByteUtil.readInt(bytes, Integer.BYTES + Long.BYTES);
    long off = ByteUtil.readLong(bytes, Integer.BYTES * 2 + Long.BYTES);
    long len = ByteUtil.readLong(bytes, Integer.BYTES * 2 + Long.BYTES*2);
    return new ShuffleIndex(mId, aId, sId, off, len);
  }
  
  public static ShuffleIndex deserializeFromStream( FSDataInputStream inputStream ) throws IOException {
    byte[] bytes = inputStream.readFully(NUM_BYTES);
    return deserializeFromBytes(bytes);
  }

  public boolean isChecksum() {
    return seqId == Constants.CHECK_SUM_SEQID;
  }

  public boolean isIndex() {
    return !isChecksum();
  }
}
