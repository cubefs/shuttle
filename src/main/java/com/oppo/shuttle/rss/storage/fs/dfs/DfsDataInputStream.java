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

package com.oppo.shuttle.rss.storage.fs.dfs;

import com.oppo.shuttle.rss.storage.fs.FSDataInputStream;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DfsDataInputStream extends FSDataInputStream {
    private final org.apache.hadoop.fs.FSDataInputStream inputStream;

    public DfsDataInputStream(org.apache.hadoop.fs.FSDataInputStream inputStream) {
        this.inputStream = inputStream;
    }

    /**
     * hdfs seek is a costly operation, skip is used here instead
     */
    @Override
    public void seek(long seekPos) throws IOException {
        long delta = seekPos - getPos();

        if (delta > 0L && delta <= 1024 * 1024) {
            skipFully(delta);
        } else if (delta != 0L) {
            forceSeek(seekPos);
        }
    }

    @Override
    public long getPos() throws IOException {
        return inputStream.getPos();
    }

    @Override
    public int read() throws IOException {
        return inputStream.read();
    }

    @Override
    public int read(@Nonnull byte[] buffer, int offset, int length) throws IOException {
        return inputStream.read(buffer, offset, length);
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }

    @Override
    public int available() throws IOException {
        return inputStream.available();
    }

    @Override
    public long skip(long n) throws IOException {
        return inputStream.skip(n);
    }

    public void forceSeek(long seekPos) throws IOException {
        inputStream.seek(seekPos);
    }

    public void skipFully(long bytes) throws IOException {
        while (bytes > 0) {
            bytes -= inputStream.skip(bytes);
        }
    }

    @Override
    public int read(ByteBuffer buffer) throws IOException {
        return inputStream.read(buffer);
    }
}