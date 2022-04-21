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

import com.oppo.shuttle.rss.storage.fs.FSDataOutputStream;

import javax.annotation.Nonnull;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class LocalDataOutputStream extends FSDataOutputStream {
    private final FileOutputStream outputStream;
    private final FileChannel channel;

    public LocalDataOutputStream(FileOutputStream outputStream) throws IOException {
        this.outputStream = outputStream;
        this.channel = outputStream.getChannel();
    }

    @Override
    public void write(int b) throws IOException {
        outputStream.write(b);
    }

    @Override
    public void write(@Nonnull byte[] b, final int off, final int len) throws IOException {
        outputStream.write(b, off, len);
    }

    @Override
    public void write(ByteBuffer buffer) throws IOException {
        channel.write(buffer);
    }

    @Override
    public void close() throws IOException {
        channel.close();
        outputStream.close();
    }

    @Override
    public void flush() throws IOException {
        outputStream.flush();
    }

    @Override
    public void sync() throws IOException {
        outputStream.getFD().sync();
    }

    @Override
    public long getPos() throws IOException {
        return channel.position();
    }
}