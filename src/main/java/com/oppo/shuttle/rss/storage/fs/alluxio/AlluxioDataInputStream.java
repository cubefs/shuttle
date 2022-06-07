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

package com.oppo.shuttle.rss.storage.fs.alluxio;

import alluxio.client.file.FileInStream;
import com.oppo.shuttle.rss.storage.fs.FSDataInputStream;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;

public class AlluxioDataInputStream extends FSDataInputStream {
    private final FileInStream inputStream;

    public AlluxioDataInputStream(FileInStream inputStream) {
        this.inputStream = inputStream;
    }

    /**
     * seek is a costly operation, skip is used here instead
     */
    @Override
    public void seek(long seekPos) throws IOException {
        inputStream.seek(seekPos);
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
        return (int) inputStream.remaining();
    }

    @Override
    public long skip(long n) throws IOException {
        return inputStream.skip(n);
    }

    @Override
    public int read(ByteBuffer buffer) throws IOException {
        return inputStream.read(buffer);
    }
}