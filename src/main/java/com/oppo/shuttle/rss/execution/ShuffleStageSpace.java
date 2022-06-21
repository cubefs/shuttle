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
import com.oppo.shuttle.rss.common.StageShuffleId;
import com.oppo.shuttle.rss.exceptions.Ors2InvalidDataException;
import com.oppo.shuttle.rss.exceptions.Ors2InvalidDirException;
import com.oppo.shuttle.rss.messages.ShuffleMessage;
import com.oppo.shuttle.rss.metrics.Ors2MetricsConstants;
import com.oppo.shuttle.rss.storage.ShuffleFileUtils;
import com.oppo.shuttle.rss.storage.ShuffleStorage;
import com.oppo.shuttle.rss.util.ChecksumUtils;
import com.oppo.shuttle.rss.util.ShuffleUtils;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class ShuffleStageSpace {
  private static final Logger logger = LoggerFactory.getLogger(ShuffleStageSpace.class);

  private final StageShuffleId stageShuffleId;
  private final int fileStartIndex;

  /**
   * Shuffle writer for partition
   * key: partitionId
   * value: shuffle writer
   */
  private final Map<Integer, ShufflePartitionUnsafeWriter> dataWriters = new ConcurrentHashMap<>();

  //<partitionId, <mapId, checksum>>
  private Map<Integer, Map<Integer, Long>> mapChecksum;

  //<partitionId, <mapId, dataSize>>
  private Map<Integer, Map<Integer, Long>> mapDataSize;

  // check coming data checkSum and dataSize in shuffle worker side, configurable
  private final boolean checkDataInShuffleWorker;

  private final String rootDir;
  private final String stageFinalizedPath;
  private final String appCompletePath;
  private final AtomicReference<Boolean> finalized = new AtomicReference<>(false);

//  Map<attempt, Map<partition, List<Checksum>>>
  private Map<Long, Map<Integer, LinkedList<Checksum>>> checksumBuffer = new HashMap<>(2);

  private long checksumBufferedCount;

  private final Ors2AbstractExecutorService<Runnable> partitionExecutor;

  private final ShuffleStorage storage;

  private final String serverId;

  public ShuffleStageSpace(
          Ors2AbstractExecutorService<Runnable> executor,
          ShuffleStorage storage,
          String serverId,
          StageShuffleId stageShuffleId,
          int fileStartIndex,
          boolean checkDataInShuffleWorker) {
    this.partitionExecutor = executor;
    this.storage = storage;
    this.stageShuffleId = stageShuffleId;
    this.rootDir = storage.getRootDir();
    this.serverId = serverId;
    this.fileStartIndex = fileStartIndex;
    this.checkDataInShuffleWorker = checkDataInShuffleWorker;
    if (checkDataInShuffleWorker) {
      this.mapChecksum  = new ConcurrentHashMap<>();
      this.mapDataSize  = new ConcurrentHashMap<>();
    }
    stageFinalizedPath = ShuffleFileUtils.getStageCompleteSignPath(rootDir, stageShuffleId).toString();
    appCompletePath = ShuffleFileUtils
            .getAppCompleteSignPath(rootDir, stageShuffleId.getAppId(), stageShuffleId.getAppAttempt()).toString();
  }

  public synchronized void addCheckSum(int mapId, long attemptId, ShuffleMessage.UploadPackageRequest.CheckSums checkSums) {
    try {
      checksumBufferedCount += checkSums.getChecksumsCount();
      for (int i = 0; i < checkSums.getChecksumsCount(); i++) {
        int partitionId = checkSums.getChecksumPartitions(i);
        long checkSum = checkSums.getChecksums(i);

        if (checkDataInShuffleWorker) {
          Map<Integer, Long> partitionCS = mapChecksum.computeIfAbsent(partitionId, key -> new ConcurrentHashMap<>());
          long storeChecksum = partitionCS.getOrDefault(mapId, -1L);
          if (storeChecksum != checkSum) {
            logger.warn("Store data checksum not equal checksumMsg chesksum, shuffleId: {}" +
                            "store: {}, comming chesksum:{}, partitionId:{}, mapId: {}",
                    this.stageShuffleId, storeChecksum, checkSum, partitionId, mapId);
          }

          Map<Integer, Long> partitionDS = mapDataSize.computeIfAbsent(partitionId, key -> new ConcurrentHashMap<>());
          long dataSize = partitionDS.getOrDefault(mapId, 0L);
          logger.info("CheckDataSize: shuffleId: {}, partitionId: {}, mapId: {}, dataSize: {}",
                  stageShuffleId, partitionId, mapId, dataSize);
        }
        Map<Integer, LinkedList<Checksum>> partitionChecksums = checksumBuffer.computeIfAbsent(attemptId,
                key -> new HashMap<>(Constants.SHUFFLE_PARTITION_COUNT_DEFAULT));
        LinkedList<Checksum> checksums = partitionChecksums.computeIfAbsent(partitionId, key -> new LinkedList<>());
        checksums.addLast(new Checksum(mapId, checkSum));

        // dump single partition checksum
        if (checksums.size() > Constants.SHUFFLE_PARTITION_INDEX_COUNT_DUMP_THRESHOLD) {
          checksumBufferedCount -= checksums.size();
          submitChecksum(partitionId, attemptId, checksums);
          partitionChecksums.remove(partitionId);
        }
      }

      if (checksumBufferedCount >= Constants.SHUFFLE_STAGE_INDEX_COUNT_DUMP_THRESHOLD) {
        logger.info("SubmitChecksum shuffleId: {}, mapId: {}, checksumSize: {},",
                stageShuffleId, mapId, checksumBufferedCount);
        submitChecksum();
        checksumBufferedCount = 0;
      }
    } catch (Exception e) {
      logger.warn("AddChecksum exception", e);
      e.printStackTrace();
    }
  }

  public synchronized void submitChecksum(int partitionId, long attemptId, List<Checksum> checksums) {
    ShufflePartitionUnsafeWriter writer = getOrCreateWriter(partitionId);
    execute(partitionId, () -> {
      writer.writeCheckSum(checksums, attemptId);
    });
  }

  public synchronized void submitChecksum() {
    for(Map.Entry<Long, Map<Integer, LinkedList<Checksum>>> en : checksumBuffer.entrySet()) {
      long attemptId = en.getKey();
      Iterator<Map.Entry<Integer, LinkedList<Checksum>>> it = en.getValue().entrySet().iterator();

      while (it.hasNext()) {
        Map.Entry<Integer, LinkedList<Checksum>> partitionChecksums = it.next();

        int partitionId = partitionChecksums.getKey();
        ShufflePartitionUnsafeWriter writer = getOrCreateWriter(partitionId);
        execute(partitionId, () -> writer.writeCheckSum(partitionChecksums.getValue(), attemptId));
        it.remove();
      }
    }

    logger.info("submit checksum, checksum size {}", checksumBufferedCount);
  }


  public synchronized StageShuffleId getStageShuffleId() {
    return stageShuffleId;
  }

  public void dataComing(int partitionId, byte[] blockData, int mapId, long attemptId, int seqId) {
    int length = blockData.length;
    try {
      ShufflePartitionUnsafeWriter partitionWriter = getOrCreateWriter(partitionId);

        if (checkDataInShuffleWorker && length > 0) {
          Map<Integer, Long> partitionChecksum = mapChecksum.computeIfAbsent(partitionId, key -> new ConcurrentHashMap<>());
          Map<Integer, Long> partitionDataSize = mapDataSize.computeIfAbsent(partitionId, key -> new ConcurrentHashMap<>());
          long dataSize = partitionDataSize.computeIfAbsent(mapId, key->0L);
          dataSize += length;
          long checksum = partitionChecksum.computeIfAbsent(mapId, key->0L);
          checksum += ChecksumUtils.getCRC32Checksum(blockData);
          partitionChecksum.put(mapId, checksum);
          partitionDataSize.put(mapId, dataSize);
          logger.info("compute checksum, shuffleId: {}, partition:{}, mapId:{}, dataSize:{}, checksum:{}",
            stageShuffleId, partitionId, mapId, length, checksum);
        }

        execute(partitionId, () -> partitionWriter.writeData(blockData, mapId, attemptId, seqId));
    } catch (Exception e) {
      logger.error("Exception in processing coming partitionBlockData:", e);
    } finally {
      Ors2MetricsConstants.bufferedDataSize.dec(length);
    }
  }

  public ShufflePartitionUnsafeWriter getOrCreateWriter(int partition) {
    if (partition < 0) {
      throw new Ors2InvalidDataException("Invalid partition: " + partition);
    }

    if (Strings.isNullOrEmpty(rootDir)) {
      throw new Ors2InvalidDirException("Invalid root dir, null or empty");
    }

    ShufflePartitionUnsafeWriter writer = dataWriters.get(partition);
    if (writer != null) {
      return writer;
    }

    PartitionShuffleId appShufflePartitionId = new PartitionShuffleId(
            stageShuffleId, partition);

    return dataWriters.computeIfAbsent(partition, p -> {
      String path = ShuffleFileUtils.getShuffleFilePath(
        rootDir, stageShuffleId, partition, serverId);
      return new ShufflePartitionUnsafeWriter(appShufflePartitionId,
      path, fileStartIndex, storage);
    });
  }

  public void finalizeStage() {
    submitChecksum();
    checksumBuffer = new HashMap<>();

    dataWriters.forEach((partitionId, writer) -> {
      execute(partitionId, writer::finalizeDataAndIndex);
    });

    finalized.set(true);
  }

  public void execute(int partitionId, Runnable run) {
     int executorId = ShuffleUtils.generateShuffleExecutorIndex(stageShuffleId.getAppId(), stageShuffleId.getShuffleId(),
            partitionId, partitionExecutor.getPoolSize());
     partitionExecutor.execute(executorId, run);
  }

  public boolean finalizedFileExists() {
    return storage.exists(stageFinalizedPath);
  }

  public boolean appCompleteFileExists() {
    return storage.exists(appCompletePath);
  }

  public void setFinalized(boolean finalized) {
    this.finalized.set(finalized);
  }

  public boolean isFinalized() {
    return finalized.get();
  }

  public void clearDataFile() {
    dataWriters.forEach((partitionId, writer) -> {
      execute(partitionId, writer::destroy);
    });
  }

  public synchronized void finalizeWriters() {
    try {
      for (ShufflePartitionUnsafeWriter writer : dataWriters.values()) {
        writer.finalizeDataAndIndex();
      }
    } catch (Exception e) {
      logger.error("Exception when finalize writers", e);
    }
  }

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("ExecutorShuffleStageState %s:", stageShuffleId.toString()));
    sb.append(String.format(", file start index: %s", fileStartIndex));
    sb.append(System.lineSeparator());
    sb.append("Writers:");
    for (Map.Entry<Integer, ShufflePartitionUnsafeWriter> entry: dataWriters.entrySet()) {
      sb.append(System.lineSeparator());
      sb.append(entry.getKey());
      sb.append("->");
      sb.append(entry.getValue());
    }
    return sb.toString();
  }
}
