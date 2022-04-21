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

import com.oppo.shuttle.rss.common.Pair;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import org.apache.parquet.Strings;

import java.nio.charset.StandardCharsets;

public class ByteUtil {

    /**
     * Merge to int to a long value
     * @param high high 32 bit of result long
     * @param low low 32 bit of result long
     * @return
     */
    public static long mergeIntToLong(int high, int low) {
        long result = high & Long.MAX_VALUE;
        result <<= 32;

        long low32 = low & Long.MAX_VALUE;

        result |= low32;

        return result;
    }

    /**
     * Write string to ByteBuf
     * Write string length firstly
     * Then write string data
     */
    public static void writeString(ByteBuf buf, String input) {
        if (Strings.isNullOrEmpty(input)) {
            buf.writeInt(-1);
            return;
        }

        byte[] byteArray = input.getBytes(StandardCharsets.UTF_8);
        buf.writeInt(byteArray.length);
        buf.writeBytes(byteArray);
    }

    public static final String readString(ByteBuf byteBuf) {
        int len = byteBuf.readInt();
        if (len == -1) {
            return null;
        }

        byte[] byteArray = new byte[len];
        byteBuf.readBytes(byteArray);
        return new String(byteArray, StandardCharsets.UTF_8);
    }

    public static final byte[] readBytes(ByteBuf buf) {
        if (buf.hasArray()) {
            return buf.array();
        }

        byte[] byteArray = new byte[buf.readableBytes()];
        buf.readBytes(byteArray);
        return byteArray;
    }

    public static final Pair<Integer, byte[]> readBytes(ByteBuf buf, boolean copy) {
        if (copy) {
            return new Pair<>(0, readBytes(buf));
        }
        int length = buf.readableBytes();
        byte[] array;
        int offset;
        if (buf.hasArray()) {
            array = buf.array();
            offset = buf.arrayOffset() + buf.readerIndex();
        } else {
            array = ByteBufUtil.getBytes(buf, buf.readerIndex(), length, false);
            offset = 0;
        }
        return new Pair<>(offset, array);
    }

    public static final int readInt(byte[] bytes, int index) {
        return (bytes[index] & 0xff) << 24 |
                (bytes[index + 1] & 0xff) << 16 |
                (bytes[index + 2] & 0xff) <<  8 |
                bytes[index + 3] & 0xff;
    }

    public static final long readLong(byte[] bytes, int index) {
        return ((long) bytes[index] & 0xff) << 56 |
                ((long) bytes[index + 1] & 0xff) << 48 |
                ((long) bytes[index + 2] & 0xff) << 40 |
                ((long) bytes[index + 3] & 0xff) << 32 |
                ((long) bytes[index + 4] & 0xff) << 24 |
                ((long) bytes[index + 5] & 0xff) << 16 |
                ((long) bytes[index + 6] & 0xff) <<  8 |
                (long) bytes[index + 7] & 0xff;
    }


    public static void main(String[] args) {
        int a = 1000;
        int b = 1000;
        long ml = mergeIntToLong(a, b);
        System.out.println(ml);
        String merged = Long.toBinaryString(mergeIntToLong(a,b));
        System.out.println(merged);
        System.out.println(merged.length());
    }
}
