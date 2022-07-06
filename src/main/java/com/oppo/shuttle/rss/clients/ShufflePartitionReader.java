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

package com.oppo.shuttle.rss.clients;

import com.carrotsearch.hppc.IntLongHashMap;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.procedures.IntLongProcedure;
import com.oppo.shuttle.rss.common.Constants;
import com.oppo.shuttle.rss.common.ShuffleDataBlock;
import com.oppo.shuttle.rss.common.SleepWaitTimeout;
import com.oppo.shuttle.rss.common.StageShuffleId;
import com.oppo.shuttle.rss.exceptions.Ors2ChecksumException;
import com.oppo.shuttle.rss.exceptions.Ors2Exception;
import com.oppo.shuttle.rss.exceptions.Ors2FileException;
import com.oppo.shuttle.rss.storage.ShuffleFileStorage;
import com.oppo.shuttle.rss.storage.ShuffleFileUtils;
import com.oppo.shuttle.rss.storage.ShuffleStorage;
import com.oppo.shuttle.rss.util.ByteUtil;
import com.oppo.shuttle.rss.util.ChecksumUtils;
import com.oppo.shuttle.rss.util.FileUtils;
import com.oppo.shuttle.rss.util.ShuffleUtils;
import org.apache.spark.shuffle.ors2.Ors2ClusterConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Read partition index and data
 * One index and data file per shuffle worker of the partition
 * @author oppo
 */
public class ShufflePartitionReader implements ShuffleReader {
  private static final Logger logger = LoggerFactory.getLogger(ShufflePartitionReader.class);

  private final int shuffleId;
  private final int partitionId;
  private final long inputReadyWaitTime;

  /**
   * map task attempt to read
   */
  private final IntLongHashMap mapTaskLatestAttempt;

  private final String dataDirPrefix;
  /**
   * partition data dir path
   * example: /ors2/data/appId/shuffleId/partitionId
   */
  private final String partitionDir;

  private PartitionPipeReader currentPipeReader;

  /**
   * shuffle service dumped all pipe data and index file
   */
  private boolean allPipeDataDumped = false;

  /**
   * finished all pipe reading
   */
  private boolean allPipeFinished = false;

  /**
   * key: pipeName
   * value: PartitionPipeReader(include index/data reader and other assist info)
   */
  private final Map<String, PartitionPipeReader> pipesReader = new HashMap<>(4);

  /**
   * key: int, mapId
   * value: long, checksum of map
   */
  private final IntLongHashMap mapCheckSumFromIndex;
  private final IntLongHashMap readingDataCheckSum;

  private final double mockErrorProbability;
  Map<String, ShuffleFileInfo> indexDataFileInfo = new HashMap<>(4);

  private final ShuffleStorage storage;

  private final LongHashSet packageIndex;

  public ShufflePartitionReader(Ors2ClusterConf clusterConf, StageShuffleId stageShuffleId,
                                int partitionId, int startMapIndex, int endMapIndex,
                                long inputReadyWaitTime, double mockErrorProbability, IntLongHashMap mapTaskLatestAttempt) {
    storage = new ShuffleFileStorage(clusterConf.rootDir(), clusterConf.fsConfBean());
    this.dataDirPrefix = storage.getRootDir();
    this.shuffleId = stageShuffleId.getShuffleId();
    this.partitionId = partitionId;
    this.inputReadyWaitTime = inputReadyWaitTime;
    this.mapTaskLatestAttempt = mapTaskLatestAttempt;
    this.partitionDir = ShuffleFileUtils.getShuffleFileDir(dataDirPrefix, stageShuffleId, partitionId);
    this.mapCheckSumFromIndex = new IntLongHashMap(512);
    this.readingDataCheckSum = new IntLongHashMap(512);
    this.mockErrorProbability = mockErrorProbability;
    this.packageIndex = new LongHashSet(1024);
    logger.info("endMapIndex - startMapIndex: {}, start:{}, end:{}, mapTaskLatestAttempt count:{}",
            endMapIndex - startMapIndex,startMapIndex,endMapIndex,mapTaskLatestAttempt.size());
    initPipeReaders();
  }

  public ShuffleFileInfo.ShuffleFileType checkShuffleFileName(String fileName) {
    if (!fileName.startsWith(Constants.SHUFFLE_FILE_PREFIX)) {
      return ShuffleFileInfo.ShuffleFileType.SHUFFLE_INVALID_TYPE;
    }
    if (fileName.endsWith(Constants.SHUFFLE_DATA_FILE_POSTFIX)) {
      return ShuffleFileInfo.ShuffleFileType.SHUFFLE_DATA_TYPE;
    }
    if (fileName.endsWith(Constants.SHUFFLE_FINAL_DATA_FILE_POSTFIX)) {
      return ShuffleFileInfo.ShuffleFileType.SHUFFLE_DATA_FINAL_TYPE;
    }
    return ShuffleFileInfo.ShuffleFileType.SHUFFLE_INVALID_TYPE;
  }

  public void waitAllPipeDataDump() {
    if (allPipeDataDumped) {
      return;
    }

    SleepWaitTimeout waitTimeout = new SleepWaitTimeout(inputReadyWaitTime);
    int waitNumber = 0;
    while (!allPipeDataDumped) {
      try {
        long sleepTime = Math.min(Constants.POLL_WAIT_MS, ((++waitNumber / 5) + 1) * 50L);
        waitTimeout.sleepAdd(sleepTime);
        initPipeReaders();
      } catch (TimeoutException e) {
        throw new Ors2Exception("Waiting for timeout. If this is normal, it is recommended to increase the value of" +
                " spark.shuffle.rss.read.waitFinalizeTimeout, the current configuration is: " + inputReadyWaitTime + " ms");
      }
    }
    logger.info("Wait for dumping data cost {} ms({} times) millis wait for dumping data",
            waitTimeout.getDurationMs(), waitNumber);
  }

  @Override
  public ShuffleDataBlock readDataBlock() throws IOException {

    waitAllPipeDataDump();

    if (currentPipeReader == null) {
      logger.info("All pipe isEmpty");
      return null;
    }

    while (currentPipeReader.skippable()) {
      currentPipeReader.mergeAllChecksumToSummary(mapCheckSumFromIndex);
      currentPipeReader.close();

      if (!currentPipeReader.isFinished()) {
        currentPipeReader.setFinished();
      }
      switchCurrentPipeReader();

      if (allPipeFinished) {
        logger.info("All pipe reader finished");
        checkAllMapData();
        return null;
      }
    }

    ShuffleDataBlock dataBlock = currentPipeReader.readDataBlock();
    if (dataBlock != null) {
      if (isDataBlockDuplicated(dataBlock)){
        logger.info("Data block {} is duplicate, read next", dataBlock);
        dataBlock = readDataBlock();
      }

      long dataChecksum = readingDataCheckSum.get(dataBlock.getTaskId());
      dataChecksum += ChecksumUtils.getCRC32Checksum(dataBlock.getData(),dataBlock.getLength());
      readingDataCheckSum.put(dataBlock.getTaskId(), dataChecksum);
    }

    return dataBlock;
  }

  @Override
  public long getShuffleReadBytes() {
    long readBytes = pipesReader.values().stream()
            .map(PartitionPipeReader::getShuffleReadBytes)
            .reduce(0L, Long::sum);
    logger.debug("getShuffleReadBytes readBytes: {}", readBytes);
    return readBytes;
  }

  @Override
  public void close() {
    try {
      for (Map.Entry<String, PartitionPipeReader> en : pipesReader.entrySet()) {
        en.getValue().close();
      }
    } catch (Exception e) {
      logger.warn("Exception closing partition pipe reader, shuffleId: {}, partition: {}", shuffleId, partitionId);
      e.printStackTrace();
    }
  }

  public void checkAllMapData() {
    logger.info("Begin check all map data checksum, shuffleId: {}, partitionId: {}, index checksums: {}, " +
                    "reading checksums:{}, mapTaskLatestAttempt.size:{}",
            shuffleId, partitionId, mapCheckSumFromIndex.size(), readingDataCheckSum.size(), mapTaskLatestAttempt.size());
    Set<Integer> invalidMap = new HashSet<>();

    // mock exception for test
    ShuffleUtils.randomErrorInjection(this.mockErrorProbability);

    mapTaskLatestAttempt.forEach((IntLongProcedure) (mapId, attempt) -> {
      if (!mapCheckSumFromIndex.containsKey(mapId)) {
        invalidMap.add(mapId);
        logger.warn("Not found checksum {} for mapId {} from index", partitionDir, mapId);
      } else if (!readingDataCheckSum.containsKey(mapId)) {
        if (mapCheckSumFromIndex.get(mapId) > Constants.EMPTY_CHECKSUM_DEFAULT) {
          invalidMap.add(mapId);
          logger.warn("Not found checksum {} for mapId: {} from reading data, index checksum: {}",
                  mapId, partitionDir, mapCheckSumFromIndex.get(mapId));
        }
      } else if (mapCheckSumFromIndex.get(mapId) != readingDataCheckSum.get(mapId)) {
        invalidMap.add(mapId);
        logger.warn("Checksum {} for mapId: {}, index and reading data not equal", partitionDir, mapId);
      }
    });

    if (!invalidMap.isEmpty()) {
      for (Integer mapId : invalidMap) {
        logger.error("Checksum {} invalid mapId: {}, actualChecksum {}, fromIdx {}",
                partitionDir, mapId, readingDataCheckSum.get(mapId), mapCheckSumFromIndex.get(mapId));
      }
      throw new Ors2ChecksumException("Reading data checksum not matching index checksum, partition: " + partitionDir, invalidMap);
    }
  }

  public int getPipesReadCount() {
    return pipesReader.size();
  }

  /**
   * Generate Map<pipeName, ShuffleFileInfo>
   */
  private void generateIndexDataInfo(File baseDir) {
    List<File> files = storage.listAllFiles(baseDir.getPath())
            .stream().map(File::new).collect(Collectors.toList());

    for (File file: files) {
      String fileName = file.getName();
      logger.info("FileName: {}", file.getPath());
      ShuffleFileInfo.ShuffleFileType fileType = checkShuffleFileName(fileName);
      if (fileType == ShuffleFileInfo.ShuffleFileType.SHUFFLE_INVALID_TYPE) {
        throw new Ors2FileException("Invalid file name for shuffle data or index: " + partitionDir + "/" + fileName);
      }

      String partitionPipeName = fileName.substring(0, fileName.lastIndexOf('.'));
      ShuffleFileInfo indexDataFile = indexDataFileInfo.computeIfAbsent(
              partitionPipeName, key -> new ShuffleFileInfo());

      switch (fileType) {
        case SHUFFLE_DATA_TYPE:
          File finalDatFile = FileUtils.getFinalFile(file);
          indexDataFile.setDataFile(finalDatFile);
          indexDataFile.setDatFileType(ShuffleFileInfo.ShuffleFileType.SHUFFLE_DATA_TYPE);
          break;
        case SHUFFLE_DATA_FINAL_TYPE:
          indexDataFile.setDataFile(file);
          indexDataFile.setDatFileType(ShuffleFileInfo.ShuffleFileType.SHUFFLE_DATA_FINAL_TYPE);
          break;
        default:
          break;
      }
    }
  }

  private void initPipeReader(String pipeName, ShuffleFileInfo shuffleFileInfo) {
    PartitionPipeReader reader = pipesReader.computeIfAbsent(pipeName, key ->
            new PartitionPipeReader(storage, pipeName, shuffleFileInfo.getDataFile(), mapTaskLatestAttempt));
    reader.readIndex();
    shuffleFileInfo.setReaderInited(true);
    logger.info("init pipe readerinit pipe reader: {}, dataFile: {}", pipeName,
            shuffleFileInfo.getDataFile().getPath());
    if (currentPipeReader == null) {
      currentPipeReader = reader;
      logger.info("Init current pipe reader, pipeName: {}", reader.getPipeName());
    }
  }

  private boolean isPipeDataFileFinalized(ShuffleFileInfo shuffleFileInfo) {
    return shuffleFileInfo.getDatFileType() == ShuffleFileInfo.ShuffleFileType.SHUFFLE_DATA_FINAL_TYPE;
  }

  private boolean pipeFinalized(ShuffleFileInfo shuffleFileInfo) {
    return isPipeDataFileFinalized(shuffleFileInfo);
  }

  private boolean readyToInitPipeReader(ShuffleFileInfo shuffleFileInfo) {
    if (shuffleFileInfo.isReaderInited()) {
      return false;
    }

    if (pipeFinalized(shuffleFileInfo)) {
      return true;
    }

    return false;
  }

  private void updateAllPipeDataDumped() {
    logger.info("updateAllPipeDataDumped indexDataFileInfo size: {}", indexDataFileInfo.size());
    boolean noDumping = true;
    // check index data file pairs
    for(Map.Entry<String, ShuffleFileInfo> en : indexDataFileInfo.entrySet()) {
      ShuffleFileInfo shuffleFileInfo = en.getValue();

      if (readyToInitPipeReader(shuffleFileInfo)) {
        initPipeReader(en.getKey(), shuffleFileInfo);
      }

      if (!shuffleFileInfo.isReaderInited()) {
        noDumping = false;
      }
    }

    allPipeDataDumped = noDumping;
  }

  /**
   *  Generate partition pipe reader map
   *  One pipe for index/data dumped by one shuffle worker of the partition
   *  PipeName format: shuffle_{ShuffleWorkerHostName}_shuffleId_partitionId
   */
  private void initPipeReaders() {
    File baseDir = new File(partitionDir);
    if (storage.exists(baseDir.getPath())) {
      generateIndexDataInfo(baseDir);
      updateAllPipeDataDumped();
    } else {
      logger.error("Partition base directory not exist! {}", partitionDir);
      throw new Ors2FileException("Partition base directory not exist! " + partitionDir);
    }
  }

  private boolean switchCurrentPipeReader() {
    boolean found = false;

    for (Map.Entry<String, PartitionPipeReader> en : pipesReader.entrySet()) {
      PartitionPipeReader reader = en.getValue();
      if (!reader.isFinished()) {
        currentPipeReader = reader;
        found = true;
        logger.info("Current pipe reader switched, new pipe name: {}", reader.getPipeName());
        break;
      }
    }

    if (!found && allPipeDataDumped) {
      allPipeFinished = true;
    }
    logger.info("Switch pipe reader, found new pipe: {}, allPipeDumped: {}", found, allPipeDataDumped);
    return found;
  }

  private boolean isDataBlockDuplicated(ShuffleDataBlock dataBlock){
    int taskId = dataBlock.getTaskId();
    int seqId = dataBlock.getSeqId();

    long merged = ByteUtil.mergeIntToLong(taskId, seqId);

    if (packageIndex.contains(merged)) {
      return true;
    }

    if (seqId > -1) {
      packageIndex.add(merged);
    }

    return false;
  }
}
