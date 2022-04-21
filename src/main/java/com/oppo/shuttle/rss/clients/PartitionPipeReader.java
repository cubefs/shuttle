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

import com.oppo.shuttle.rss.common.ShuffleDataBlock;
import com.oppo.shuttle.rss.execution.ShuffleIndex;
import com.oppo.shuttle.rss.storage.ShuffleStorage;
import com.oppo.shuttle.rss.storage.fs.FSDataInputStream;
import com.carrotsearch.hppc.IntLongHashMap;
import com.carrotsearch.hppc.procedures.IntLongProcedure;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.Ors2Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Read index and data from dfs, one pipe for one shuffle worker of the partition.
 * TODO: add ReadClientMetrics
 * @author oppo
 */
public class PartitionPipeReader implements ShuffleReader {
    private static final Logger logger = LoggerFactory.getLogger( PartitionPipeReader.class );

    private final String pipeName;
    private boolean finished = false;
    private final File dataFile;
    private FSDataInputStream dataInStream;

    private Queue<ShuffleIndex> indexes = new LinkedList<>();

    private boolean checksumMerged = false;

    /**
     * map task attempt to read
     * key: int, mapId
     * value: long, attemptId
     */
    private IntLongHashMap mapTaskLatestAttempt;

    /**
     * key: int, mapId
     * value: long, checksum of map
     */
    private IntLongHashMap mapChecksum = new IntLongHashMap(512);

    private long totalReadDataSize = 0;

    private final ShuffleStorage storage;

    private final long bufferSize;

    public PartitionPipeReader(ShuffleStorage storage, String pipeName, File dataFile, IntLongHashMap mapTaskLatestAttempt ) {
        this.storage = storage;
        this.pipeName = pipeName;
        this.dataFile = dataFile;
        this.mapTaskLatestAttempt = mapTaskLatestAttempt;
        bufferSize = (long) SparkEnv.get().conf().get(Ors2Config.readBufferSize());
        initInputStream();
        logger.debug( "mapTaskLatestAttempt:{}", mapTaskLatestAttempt.toString() );
    }

    private void initInputStream() {
        try {
            logger.info( "initInputStream, dataFile:{}, bufferSize: {}",
                    dataFile.getPath(), bufferSize);
            dataInStream = storage.createReaderStream(dataFile.getPath(), (int) bufferSize);
        }
        catch ( Throwable e ) {
            logger.error( "Open index or data input stream failed, pipeName: {}", pipeName );
            e.printStackTrace();
        }
    }

    @Override
    public ShuffleDataBlock readDataBlock() throws IOException {
        if ( indexes.isEmpty() ) {
            logger.info( "Data of index has finished reading, pipeName: {}", pipeName );
            return null;
        }

        ShuffleIndex index = indexes.poll();
        byte[] bytes = dataInStream.readFully( (int) index.getLength() );
        totalReadDataSize += index.getLength();

        // read next index
        readNextIndex();

        if ( indexes.isEmpty() ) {
            finished = true;
        }

        logger.debug( "Read data for index: {}, bytes length: {}", index.toString(), bytes.length );
        return new ShuffleDataBlock( bytes, (int) index.getLength(), index.getMapId(), index.getAttemptId(), index.getSeqId());
    }

    @Override
    public long getShuffleReadBytes() {
        return totalReadDataSize;
    }

    @Override
    public void close() {
        try {
            if (dataInStream != null ) {
                dataInStream.close();
                dataInStream = null;
                logger.info("close pipe file: {}", dataFile.getPath());
            }
        } catch ( IOException e ) {
            logger.error( "Exception in closing input stream, pipeName: {}", pipeName, e);
        }
    }

    public void readNextIndex() throws IOException {

        while ( true ) {
            ShuffleIndex index;
            try {
                index = ShuffleIndex.deserializeFromStream( dataInStream );
            }
            catch ( EOFException e ) {
                break;
            }
            catch ( IOException e ) {
                logger.error( "read index error,pipename:{}", pipeName, e );
                throw e;
            }

            // check index valid
            if (mapTaskLatestAttempt.containsKey(index.getMapId())) {
                Long requireAttempt = mapTaskLatestAttempt.get(index.getMapId());
                if ( requireAttempt != index.getAttemptId() ) {
                    logger.debug( "Invalid attemptId: {} for map task: {}, require attemptId: {}, index: {}",
                            index.getAttemptId(), index.getMapId(), requireAttempt, index.toString() );

                    // Checksum message cannot perform skip operation
                    if (index.isIndex())  {
                        dataInStream.skip(index.getLength());
                    }
                    continue;
                }
            } else {
                logger.debug("Invalid mapId: {}, not exist in mapTaskLatestAttempt, index: {}", index.getMapId(),
                        index.toString());
                // Checksum message cannot perform skip operation
                if (index.isIndex()) {
                    dataInStream.skip(index.getLength());
                }
                continue;
            }

            if (index.isChecksum()) {
                if ( index.getLength() > 0 ) {
                    logger.debug( "mapId: {}, put checksum: {}", index.getMapId(), index.getLength() );
                }
                mapChecksum.put( index.getMapId(), index.getLength() );
            }
            else {
                indexes.offer( index );
                break;
            }
        }
    }

    //read one
    public void readIndex() {
        try {
            readNextIndex();
        }
        catch ( Exception e ) {
            logger.error( "Exception when readIndex", e );
        }
    }

    public String getPipeName() {
        return pipeName;
    }

    public boolean isFinished() {
        return finished;
    }

    public void setFinished() {
        finished = true;
    }

    public boolean isDataIndexEmpty() {
        return indexes.isEmpty();
    }

    public boolean skippable() {
        return finished || indexes.isEmpty();
    }

    public void mergeAllChecksumToSummary( IntLongHashMap summaryChecksum ) {
        if ( !checksumMerged ) {
            mapChecksum.forEach((IntLongProcedure) summaryChecksum::put);
            checksumMerged = true;
        }
    }
}