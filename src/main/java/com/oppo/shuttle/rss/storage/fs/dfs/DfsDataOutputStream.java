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

import com.oppo.shuttle.rss.storage.fs.FSDataOutputStream;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DfsDataOutputStream extends FSDataOutputStream {
    private final org.apache.hadoop.fs.FSDataOutputStream outputStream;

    public DfsDataOutputStream(org.apache.hadoop.fs.FSDataOutputStream outputStream) {
        this.outputStream = outputStream;
    }

    @Override
    public void write(int b) throws IOException {
        outputStream.write(b);
    }

    @Override
    public void write(@Nonnull byte[] bytes, int off, int len) throws IOException {
        outputStream.write(bytes, off, len);
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }

    @Override
    public long getPos() throws IOException {
        return outputStream.getPos();
    }

    @Override
    public void flush() throws IOException {
        outputStream.hflush();
    }

    @Override
    public void sync() throws IOException {
        outputStream.hsync();
    }

    public org.apache.hadoop.fs.FSDataOutputStream getDfsOutputStream() {
        return outputStream;
    }

    @Override
    public void write(ByteBuffer buffer) throws IOException {
        if (buffer.hasArray()) {
            write(buffer.array());
        } else {
            // @todo HDFS does not have nio write api, here is one more memory copy, follow-up optimization
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            write(bytes, 0, bytes.length);
        }
    }
}