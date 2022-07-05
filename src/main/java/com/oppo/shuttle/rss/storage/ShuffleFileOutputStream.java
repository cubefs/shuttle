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

package com.oppo.shuttle.rss.storage;

import com.oppo.shuttle.rss.exceptions.Ors2Exception;
import com.oppo.shuttle.rss.exceptions.Ors2FileException;
import com.oppo.shuttle.rss.metrics.Ors2MetricsConstants;
import com.oppo.shuttle.rss.storage.fs.FSDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;

/***
 * Local file based shuffle output stream.
 */
public class ShuffleFileOutputStream implements ShuffleOutputStream {
    private static final Logger logger = LoggerFactory.getLogger(ShuffleFileOutputStream.class);
    
    private final String filePath;
    private FSDataOutputStream outputStream;
    private long writtenBytes = 0L;
    private long initFileLength = 0L;
    private AtomicBoolean closed = new AtomicBoolean(false);

    public ShuffleFileOutputStream(String filePath, FSDataOutputStream outputStream) {
        this.filePath = filePath;
        try {
            this.outputStream = outputStream;
            initFileLength = outputStream.getPos();
            logger.info("Init ShuffleFileOutputStream file: {}, initFileLength: {}", this.filePath, initFileLength);
        } catch (Throwable e) {
            throw new Ors2Exception("Failed to open or create writable file: " + this.filePath, e);
        }
    }

    @Override
    public void write(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            logger.warn("Invalid bytes to write, file {}", filePath);
            return;
        }
        int length = bytes.length;
        try {
            long start = System.currentTimeMillis();
            outputStream.write(bytes, 0, bytes.length);
            writtenBytes += length;
            long cost = System.currentTimeMillis() - start;
            Ors2MetricsConstants.dumpLatencyHistogram.observe(cost);
            if (cost > 100) {
                logger.warn("fs-flush so slow file {}, size {}, cost {} ms", filePath, length, cost);
            }
        } catch (Throwable e) {
            throw new Ors2FileException(String.format(
                    "Failed to write %s bytes to file %s with exception %s",
                    length, filePath, e.getMessage()),
                    e);
        }
    }

    @Override
    public void write(ByteBuffer buffer) {
        write(buffer.array());
    }

    @Override
    public void flush() {
        try {
            outputStream.flush();
        } catch (Throwable e) {
            throw new Ors2FileException("Failed to flush data to file, msg: " + e.getMessage(), e);
        }
    }

    @Override
    public void sync() {
        try {
            outputStream.sync();
        } catch (IOException e) {
            logger.error("Sync data to disk file failed: {} ", filePath, e);
        }
    }

    @Override
    public void close() {
        if (closed.get()) {
            return;
        }
        closed.set(true);

        try {
            logger.info("call outputStream close: {}", filePath);
            try {
                outputStream.flush();
            } finally {
                outputStream.close();
            }
        } catch (Throwable e) {
            throw new Ors2FileException(String.format("Failed to close file %s with exception %s",
                filePath, e.getMessage()), e);
        }
    }

    @Override
    public String getLocation() {
        return filePath;
    }

    @Override
    public long getWrittenBytes() {
        return initFileLength + writtenBytes;
    }

    @Override
    public boolean closed() {
        return closed.get();
    }

    @Override
    public String toString() {
        return "ShuffleFileOutputStream{" +
                "filePath='" + filePath + '\'' +
                '}';
    }

    @Override
    public FileChannel getChannel() {
        return null;
    }
}
