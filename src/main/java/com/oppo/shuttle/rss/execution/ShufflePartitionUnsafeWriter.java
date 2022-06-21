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
import com.oppo.shuttle.rss.common.PartitionShuffleId;
import com.oppo.shuttle.rss.metrics.Ors2MetricsConstants;
import com.oppo.shuttle.rss.storage.ShuffleOutputStream;
import com.oppo.shuttle.rss.storage.ShuffleStorage;
import com.oppo.shuttle.rss.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class ShufflePartitionUnsafeWriter {
    private static final Logger logger = LoggerFactory.getLogger(ShufflePartitionUnsafeWriter.class);

    private final PartitionShuffleId shufflePartitionId;
    private final String filePathBase;
    private final int fileStartIndex;
    private final ShuffleStorage storage;

    private File dataFile;
    private String dataPath;
    private ShuffleOutputStream dataOutputStream;
    private final AtomicBoolean closed = new AtomicBoolean(true);
    private long writtenBytes = 0;

    ShuffleIndexByteBuffer indexByteBuffer = new ShuffleIndexByteBuffer(ShuffleIndex.NUM_BYTES);

    public ShufflePartitionUnsafeWriter(
            PartitionShuffleId shufflePartitionId,
            String filePathBase,
            int fileStartIndex,
            ShuffleStorage storage) {
        this.shufflePartitionId = shufflePartitionId;
        this.filePathBase = filePathBase;
        this.fileStartIndex = fileStartIndex;
        this.storage = storage;

        open();
    }

    public synchronized void close() {
        if (closed.get()) {
            logger.error("Shuffle file already closed: {}, do not need to close it again", filePathBase);
            return;
        }

        try {
            logger.info("Closing shuffle data file: {}", dataOutputStream.getLocation());
            dataOutputStream.close();
            closed.set(true);
        } finally {
            try {
                Ors2MetricsConstants.partitionCurrentCount.dec();
            } catch (Exception e) {
                logger.error("dataBuffer close fail", e);
            }
        }
    }

    public synchronized boolean isClosed() {
        return closed.get();
    }

    /***
     * Get all file locations.
     * @return
     */
    public List<String> getFileLocations() {
        List<String> result = new ArrayList<>();
        result.add(dataOutputStream.getLocation());
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ShufflePartitionUnsafeWriter that = (ShufflePartitionUnsafeWriter) o;
        return fileStartIndex == that.fileStartIndex &&
                Objects.equals(shufflePartitionId, that.shufflePartitionId) &&
                Objects.equals(filePathBase, that.filePathBase);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shufflePartitionId, filePathBase, fileStartIndex);
    }

    @Override
    public String toString() {
        return "ShufflePartitionWriter{" +
                "shufflePartitionId=" + shufflePartitionId +
                ", filePathBase='" + filePathBase + '\'' +
                ", fileStartIndex='" + fileStartIndex + '\'' +
                ", closed=" + closed.get() +
                '}';
    }

    private synchronized void open() {
        if (closed.get()) {
            String parentPath = Paths.get(filePathBase).getParent().toString();
            storage.createDirectories(parentPath);
            dataPath = filePathBase + Constants.SHUFFLE_DATA_FILE_POSTFIX;

            dataFile = new File(dataPath);
            renameBackFinalFile(dataFile);
            dataOutputStream = storage.createWriterStream(dataPath, "");

            closed.set(false);
            logger.info("Opening shuffle data file: {}, dataStream: {}", dataPath, dataOutputStream.hashCode());
            Ors2MetricsConstants.partitionCurrentCount.inc();
        } else {
            logger.info("Opened, dataFile: {}", dataFile.getPath());
        }
    }

    public synchronized int writeData(byte[] shuffleData, ShuffleIndex shuffleIndex) {
        int length = shuffleData.length;

        shuffleIndex.setLength(length);
        shuffleIndex.setOffset(writtenBytes + ShuffleIndex.NUM_BYTES);
        byte[] indexData = shuffleIndex.serialize();

        dataOutputStream.write(indexData);
        dataOutputStream.write(shuffleData);
        writtenBytes += length + indexData.length;

        return length;
    }

    public synchronized int writeData(byte[] shuffleData, int mapId, long attemptId, int seqId) {
        int length = shuffleData.length;

        indexByteBuffer.clear();
        indexByteBuffer.writeInt(mapId);
        indexByteBuffer.writeLong(attemptId);
        indexByteBuffer.writeInt(seqId);
        indexByteBuffer.writeLong(writtenBytes + ShuffleIndex.NUM_BYTES);
        indexByteBuffer.writeLong(length);
        dataOutputStream.write(indexByteBuffer.getBytes());
        dataOutputStream.write(shuffleData);
        writtenBytes += shuffleData.length + ShuffleIndex.NUM_BYTES;

        return length;
    }

    public synchronized void writeCheckSum(ShuffleIndex shuffleIndex) {
        byte[] indexData = shuffleIndex.serialize();
        dataOutputStream.write(indexData);
    }

    public synchronized void writeCheckSum(List<Checksum> checksums, long attemptId) {
        for (Checksum ck : checksums) {
            indexByteBuffer.clear();
            indexByteBuffer.writeInt(ck.getMapId());
            indexByteBuffer.writeLong(attemptId);
            indexByteBuffer.writeInt(Constants.CHECK_SUM_SEQID);
            indexByteBuffer.writeLong(0);
            indexByteBuffer.writeLong(ck.getChecksum());
            dataOutputStream.write(indexByteBuffer.getBytes());
        }
    }

    private void renameBackFinalFile(File backToFile) {
        String path = backToFile.getPath();
        if (!storage.exists(path)) {
            File finalIndexFile = FileUtils.getFinalFile(backToFile);
            if (storage.exists(finalIndexFile.getPath())) {
                boolean renamed = storage.rename(finalIndexFile.getPath(), backToFile.getPath());
                logger.info("Rename back file: {}, renamed: {}, this: {}",
                        backToFile.getPath(), renamed, hashCode());
            }
        }
    }

    private void renameFileFinalized(File outputFile) {
        long start = System.currentTimeMillis();
        File newIndexFile = FileUtils.getFinalFile(outputFile);
        Path finalDirPath = Paths.get(newIndexFile.getParentFile().getPath());
        if (!storage.exists(finalDirPath.toString())) {
            storage.createDirectories(finalDirPath.toString());
        }
        if (storage.exists(newIndexFile.getPath())) {
            logger.error("Finalize data for : {}, final index file exist: {}",
                    outputFile.getPath(), newIndexFile.getName());
        }
        boolean renamed = storage.rename(outputFile.getPath(), newIndexFile.getPath());
        logger.info("Rename index file: {}, length: {}, renamed: {}, this: {}, costTime: {}",
                newIndexFile.getPath(), newIndexFile.length(), renamed, hashCode(), System.currentTimeMillis() - start);
    }


    public synchronized void finalizeDataAndIndex() {
        logger.info("finalizeData buffered file {}", dataFile.getPath());
        close();
        renameFileFinalized(dataFile);
    }

    public synchronized void destroy() {
        if (storage.exists(dataPath)){
            close();
            deleteQuietly("fail", dataPath);
        }

        String finalPath = FileUtils.getFinalFile(dataFile).getPath();
        if (storage.exists(finalPath)) {
            deleteQuietly("success", finalPath);
        }
    }

    private void deleteQuietly(String mark, String path) {
        try {
            storage.deleteFile(path);
            logger.debug("delete {} file {}", mark, path);
        } catch (Exception e) {
            logger.debug("delete {} error", path, e);
        }
    }

    public String getDataPath() {
        return dataPath;
    }

    public int getPartitionId() {
        return shufflePartitionId.getPartitionId();
    }
}
