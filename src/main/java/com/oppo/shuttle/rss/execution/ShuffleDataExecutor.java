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
import com.oppo.shuttle.rss.common.StageShuffleId;
import com.oppo.shuttle.rss.exceptions.Ors2Exception;
import com.oppo.shuttle.rss.messages.MessageConstants;
import com.oppo.shuttle.rss.messages.ShuffleData;
import com.oppo.shuttle.rss.messages.ShuffleMessage;
import com.oppo.shuttle.rss.metrics.Ors2MetricsConstants;
import com.oppo.shuttle.rss.storage.ShuffleFileStorage;
import com.oppo.shuttle.rss.storage.ShuffleStorage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultEventLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ShuffleDataExecutor {
    private static final Logger logger = LoggerFactory.getLogger(ShuffleDataExecutor.class);

    public static final long DEFAULT_APP_MEMORY_RETENTION_MILLIS = TimeUnit.HOURS.toMillis(6);

    /**
     * Storage(distribute file system) dir for shuffle data
     */
    private final String storageDir;

    // This field stores states for different applications
    // TODO: wash:change ShuffleAppState to AppState or remove appStates in future
    private final ConcurrentHashMap<String, ShuffleAppState> appState
            = new ConcurrentHashMap<>();
    
    // This field stores spaces for different shuffle stages; Shuffle like space capsule
    private final ConcurrentHashMap<StageShuffleId, ShuffleStageSpace> stageSpaces
            = new ConcurrentHashMap<>();

    private final ShuffleStorage storage;

    private final long appObjRetentionMillis;
    private final ScheduledExecutorService expireAppObjRemoveService = new DefaultEventLoop();

    // background executor service to check stage finalize
    private final ScheduledExecutorService stageFinalizedChecker = new DefaultEventLoop();

    // TODO: make dumper thread num configurable
    private final Ors2AbstractExecutorService<Runnable> partitionExecutor;

    private final String serverId;

    private final boolean checkDataInShuffleWorker;

    private transient volatile boolean stopped = false;

    /***
     * @param storageDir root directory.
     */
    public ShuffleDataExecutor(String storageDir) {
        this(new ShuffleFileStorage(storageDir), DEFAULT_APP_MEMORY_RETENTION_MILLIS,
          Constants.SHUFFLE_DATA_DUMPER_THREADS, 100, false, "localhost");
    }

    public ShuffleDataExecutor(ShuffleStorage storage,
      long appObjRetentionMillis,
      int dumperThreads,
      int dumperQueueSize,
      boolean checkDataInShuffleWorker,
      String serverId) {
        this.storageDir = storage.getRootDir();
        logger.info("Started with storageDir={}, storage={}, appObjRetentionMillis={}, dumperThreads={}, dumperQueueSize={}",
                storageDir, storage, appObjRetentionMillis, dumperThreads, dumperQueueSize);
        this.storage = storage;
        this.appObjRetentionMillis = appObjRetentionMillis;
        this.serverId = serverId;
        this.partitionExecutor = new Ors2WorkerPartitionExecutor(dumperThreads, dumperQueueSize, 0);
        this.checkDataInShuffleWorker = checkDataInShuffleWorker;
        
        this.expireAppObjRemoveService.scheduleAtFixedRate(new Runnable() {
          @Override
          public void run() {
            try {
              removeExpiredAppObj();
            } catch (Exception e) {
              logger.error("Remote expired app object exception: ", e);
            }
          }
        }, 100, 100, TimeUnit.SECONDS);

        // check stage finalized per 100 millisecond
        this.stageFinalizedChecker.scheduleAtFixedRate(this::checkFinalizedStage,
                Constants.STAGE_FINALIZED_CHECK_INTERVAL_MILLIS,
                Constants.STAGE_FINALIZED_CHECK_INTERVAL_MILLIS,
                TimeUnit.MILLISECONDS);
    }

    private void checkFinalizedStage() {
        LinkedList<ShuffleStageSpace> shuffleStageSpaces = new LinkedList<>(stageSpaces.values());

        for (ShuffleStageSpace stage : shuffleStageSpaces) {
            try {
                if (stage.shouldFinalized()) {
                    stage.finalizeStage();
                    logger.info("Finalize stage success: {}", stage.getStageShuffleId());
                } else if (stage.shouldClear()) {
                    stage.clearDataFile();
                    logger.info("Clear stage success: {}", stage.getStageShuffleId());
                } else if (stage.shouldDelete()) {
                    stageSpaces.remove(stage.getStageShuffleId());
                    logger.info("Delete stage success: {}", stage.getStageShuffleId());
                }
            } catch (Exception e) {
                logger.error("Finalize or clear stage fail: {}", stage.getStageShuffleId(), e);
            }
        }
    }

    public void processUploadPackage(ChannelHandlerContext ctx, ShuffleData shuffleData,
                                     StageShuffleId stageShuffleId, String connInfo) {
        if (stopped) {
            throw new Ors2Exception("Shuffle executor has been shutdown");
        }

        if (shuffleData == null) {
            ByteBuf buf = ctx.alloc().buffer(1);
            buf.writeByte(MessageConstants.RESPONSE_STATUS_INVALID_PACKAGE);
            ctx.writeAndFlush(buf).addListener(ChannelFutureListener.CLOSE);
            logger.warn("Invalid package, connectionInfo: {}", connInfo);
            return;
        }
        dataComing(ctx, stageShuffleId, shuffleData, connInfo);
    }

    public String getStorageDir() {
        return storageDir;
    }

    /**
     * process incoming shuffle block package data
     */
    public void dataComing(ChannelHandlerContext ctx, StageShuffleId appShuffleId, ShuffleData shuffleData,
      String connInfo) {
        // TODO: add metrics info
        //logger.debug("dataComing: {}", uploadPackage.toString());
        ShuffleMessage.UploadPackageRequest uploadPackage = shuffleData.getUploadPackage();
        int mapId = uploadPackage.getMapId();
        long attemptId = uploadPackage.getAttemptId();
        int seqId = uploadPackage.getSeqId();
        ShuffleStageSpace shuffleStageSpace = getStageSpace(appShuffleId);

        // An exception occurred in the stage writing, and the connection was closed. Make spark tasks fail fast
        try  {
            shuffleStageSpace.checkHasError();
        } catch (Exception e) {
            logger.error("stage exception", e);
            ctx.close();
            return;
        }

        // If the task is retried,
        // it can be seen that the stage has ended, but the previous task may
        // still have data sent to the shuffle worker, and this part of the data needs to be discarded.
        if (!shuffleStageSpace.isRunning()) {
            logger.warn("The stage has ended(discarded data), appShuffleId {}, mapId {}, attemptId {}, seqId {}",
                    appShuffleId, mapId, attemptId, seqId);
            return;
        }

        shuffleData.getPartitionBlocks().forEach(pbd -> {
            Ors2MetricsConstants.bufferedDataSize.inc(pbd.getData().length);
            Ors2MetricsConstants.writeTotalBytes.inc(pbd.getData().length);
            shuffleStageSpace.dataComing(pbd.getPartition(), pbd.getData(), mapId, attemptId, pbd.getSeqId());
        });


        //process checksum msg
        if (uploadPackage.hasCheckSums()) {
            ShuffleMessage.UploadPackageRequest.CheckSums checkSums = uploadPackage.getCheckSums();
            if (checkSums.getChecksumPartitionsCount() != checkSums.getChecksumsCount()) {
                throw new Ors2Exception("Invalid checksum, connectionInfo: " + connInfo);
            }

            shuffleStageSpace.addCheckSum(mapId, attemptId, checkSums);
            logger.debug("AppShuffleId: {}, Checksum partition count: {}, checksum count: {}",
                    appShuffleId, checkSums.getChecksumPartitionsCount(), checkSums.getChecksumsCount());
        }
    }

    /**
     * Stop ShuffleWorker data executor
     */
    public synchronized void stop() {
        if (stopped) {
            return;
        }
        stopped = true;

        System.out.printf("%s Stop shuffle executor during shutdown%n", LocalDateTime.now());

        stageFinalizedChecker.shutdown();
        expireAppObjRemoveService.shutdown();

        System.out.printf("%s force submitChecksum%n", LocalDateTime.now());
        for (ShuffleStageSpace ss : stageSpaces.values()) {
            ss.submitChecksum();
        }

        System.out.printf("%s force close partitionExecutor%n", LocalDateTime.now());
        partitionExecutor.shutdown();

        System.out.printf("%s force finalizeStage%n", LocalDateTime.now());
        for (Iterator<ShuffleStageSpace> sit = stageSpaces.values().iterator(); sit.hasNext(); ) {
            ShuffleStageSpace ss = sit.next();
            if (ss.isRunning()) {
                ss.finalizeStage();
            }
            sit.remove();
        }

        System.out.printf("%s Stopped shuffle executor during shutdown%n", LocalDateTime.now());
    }

    /**
     * Update aliveness last time for the app
     * @param appId
     */
    public ShuffleAppState updateAppAliveness(String appId) {
        ShuffleAppState shuffleAppState = getShuffleAppState(appId);
        shuffleAppState.updateAppLiveLastTime();
        return shuffleAppState;
    }

    private ShuffleAppState getShuffleAppState(String appId) {
        ShuffleAppState shuffleAppState = appState.get(appId);
        if (shuffleAppState != null) {
            return shuffleAppState;
        }
        ShuffleAppState newAppState = new ShuffleAppState(appId);
        shuffleAppState = appState.putIfAbsent(appId, newAppState);
        Ors2MetricsConstants.appTotalCount.inc(); // app total
        if (shuffleAppState == null) {
            return newAppState;
        } else {
            return shuffleAppState;
        }
    }
    
    private ShuffleStageSpace getStageSpace(StageShuffleId stageShuffleId) {
        return stageSpaces.computeIfAbsent(stageShuffleId, key-> {
            ShuffleStageSpace stageSpace = new ShuffleStageSpace(partitionExecutor, storage,
                    serverId, stageShuffleId, 0, checkDataInShuffleWorker);
            logger.info("add runningStage: {}", stageShuffleId);
            return stageSpace;
        });
    }

    private void removeExpiredAppObj() {
        long currentMillis = System.currentTimeMillis();
        List<String> expiredApps = new ArrayList<>();
        for (Map.Entry<String, ShuffleAppState> entry: appState.entrySet()) {
            if (entry.getValue().getAppLiveLastTime() < currentMillis - appObjRetentionMillis) {
                String id = entry.getKey();
                expiredApps.add(id);
                logger.info("Found expired app id : {} to remove", id);
            }
        }

        for (String appId : expiredApps) {
            appState.remove(appId);
            List<StageShuffleId> expiredAppObjIds = stageSpaces.keySet()
                    .stream()
                    .filter(a->a.getAppId().equals(appId))
                    .collect(Collectors.toList());
            List<ShuffleStageSpace> removedAppShuffleStageStates =
                    expiredAppObjIds.stream()
                            .map(stageSpaces::remove)
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());

            if (removedAppShuffleStageStates.isEmpty()) {
                continue;
            }

            removedAppShuffleStageStates.forEach(stage -> {
                logger.info("Removed expired stage: {}", stage.getStageShuffleId());
                stage.clearDataFile();
            });
            logger.info("Removed expired app obj from internal state: {}, number of app shuffle id: {}", appId,
                    expiredAppObjIds.size());
        }
    }
}