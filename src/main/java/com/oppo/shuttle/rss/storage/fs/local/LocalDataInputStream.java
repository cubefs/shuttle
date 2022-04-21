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

package com.oppo.shuttle.rss.storage.fs.local;

import com.oppo.shuttle.rss.storage.fs.FSDataInputStream;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class LocalDataInputStream extends FSDataInputStream {
    private final FileInputStream inputStream;
    private final FileChannel channel;

    public LocalDataInputStream(File file) throws IOException {
        this.inputStream = new FileInputStream(file);
        channel = inputStream.getChannel();
    }

    @Override
    public void seek(long desired) throws IOException {
        if (desired != getPos()) {
            channel.position(desired);
        }
    }

    @Override
    public long getPos() throws IOException {
        return channel.position();
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
        channel.close();
        inputStream.close();
    }

    @Override
    public int available() throws IOException {
        return inputStream.available();
    }

    @Override
    public long skip(final long n) throws IOException {
        return inputStream.skip(n);
    }

    @Override
    public int read(ByteBuffer buffer) throws IOException {
        return channel.read(buffer);
    }
}