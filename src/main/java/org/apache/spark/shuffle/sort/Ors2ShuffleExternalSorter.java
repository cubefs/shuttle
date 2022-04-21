/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.sort;

import com.oppo.shuttle.rss.common.RandomSortPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.internal.config.package$;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.memory.TooLargePageException;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.serializer.DummySerializerInstance;
import org.apache.spark.shuffle.Ors2Config;
import org.apache.spark.shuffle.ors2.Ors2BlockBuffer;
import org.apache.spark.shuffle.ors2.Ors2BlockManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.LinkedList;

/**
 * @see ShuffleExternalSorter
 * Most of the methods of ShuffleExternalSorter are private and cannot be inherited.
 * A rewrite is made here, the core code has not been modified,
 * the unnecessary merge method is deleted, and the spill data is modified to the network
 */
final public class Ors2ShuffleExternalSorter extends MemoryConsumer {
    private static final Logger logger = LoggerFactory.getLogger(Ors2ShuffleExternalSorter.class);

    private final int numPartitions;
    private final TaskMemoryManager taskMemoryManager;
    private final Ors2BlockManager blockManager;
    private final TaskContext taskContext;

    /**
     * Force this sorter to spill when there are this many elements in memory.
     */
    private final int numElementsForSpillThreshold;

    private final long memoryForSpillThreshold;

    private long recordUsedMemory = 0L;

    /**
     * BlockSize limit for send shuffle-worker, default 1mb
     */
    private final int writeBlockSize;

    /**
     * Memory pages that hold the records being sorted. The pages in this list are freed when
     * spilling, although in principle we could recycle these pages across spills (on the other hand,
     * this might not be necessary if we maintained a pool of re-usable pages in the TaskMemoryManager
     * itself).
     */
    private final LinkedList<MemoryBlock> allocatedPages = new LinkedList<>();

    /** Peak memory used by this sorter so far, in bytes. **/
    private long peakMemoryUsedBytes;

    // These variables are reset after spilling:
    @Nullable private ShuffleInMemorySorter inMemSorter;
    @Nullable private MemoryBlock currentPage = null;
    private long pageCursor = -1;

    private final Ors2BlockBuffer blockBuffer;

    private byte[] writeBuffer;

    private int memorySpillCount = 0;

    private final RandomSortPartition partitionSort;

    Ors2ShuffleExternalSorter(
            Ors2BlockManager blockManager,
            TaskMemoryManager memoryManager,
            TaskContext taskContext,
            int initialSize,
            int numPartitions,
            SparkConf conf) {
        super(memoryManager,
                (int) Math.min(PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES, memoryManager.pageSizeBytes()),
                memoryManager.getTungstenMemoryMode());
        this.blockManager = blockManager;
        this.taskMemoryManager = memoryManager;
        this.taskContext = taskContext;
        this.numPartitions = numPartitions;
        this.numElementsForSpillThreshold =
                (int) conf.get(package$.MODULE$.SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD());
        this.memoryForSpillThreshold = (long) conf.get(Ors2Config.shuffleSpillMemoryThreshold());

        this.inMemSorter = new ShuffleInMemorySorter(
                this, initialSize, conf.getBoolean("spark.shuffle.sort.useRadixSort", true));
        this.peakMemoryUsedBytes = getMemoryUsage();

        this.writeBlockSize = (int) (long) conf.get(Ors2Config.writeBlockSize());
        this.writeBuffer = new byte[1024 * 1024];
        this.blockBuffer = blockManager.getBlockBuffer(DummySerializerInstance.INSTANCE, writeBlockSize, blockManager.maxBufferSize());

        this.partitionSort = new RandomSortPartition(numPartitions);
    }

    /**
     * send data to shuffle worker。
     */
    private void writeToServer() {
        assert(inMemSorter != null);
        // This call performs the actual sort.
        final ShuffleInMemorySorter.ShuffleSorterIterator sortedRecords = inMemSorter.getSortedIterator();

        // If there are no sorted records, so we don't need to send package to shuffle-worker.
        if (!sortedRecords.hasNext()) {
            return;
        }

        int currentPartition = -1;
        final int uaoSize = UnsafeAlignedOffset.getUaoSize();
        while (sortedRecords.hasNext()) {
            sortedRecords.loadNext();
            final int partition = sortedRecords.packedRecordPointer.getPartitionId();
            assert (partition >= currentPartition);
            if (partition != currentPartition) {
                // Switch to the new partition
                if (currentPartition != -1) {
                    blockManager.spill(partitionSort.restore(currentPartition), blockBuffer.toBytes());
                }
                currentPartition = partition;
            }

            final long recordPointer = sortedRecords.packedRecordPointer.getRecordPointer();
            final Object recordPage = taskMemoryManager.getPage(recordPointer);
            final long recordOffsetInPage = taskMemoryManager.getOffsetInPage(recordPointer);
            int dataRemaining = UnsafeAlignedOffset.getSize(recordPage, recordOffsetInPage);
            long recordReadPosition = recordOffsetInPage + uaoSize; // skip over record length

            // TODO: parallelize compress and copyMem as shuffle-worker group
            while (dataRemaining > 0) {
                final int toTransfer = Math.min(writeBlockSize, dataRemaining);
                Platform.copyMemory(
                        recordPage, recordReadPosition, writeBuffer, Platform.BYTE_ARRAY_OFFSET, toTransfer);
                blockBuffer.write(writeBuffer, 0, toTransfer);
                recordReadPosition += toTransfer;
                dataRemaining -= toTransfer;
            }
            blockManager.recordsAdd();

            // Determine whether the current data size reaches a block. If it arrives, it needs to send data out.
            if (blockBuffer.position() >= writeBlockSize) {
                blockManager.spill(partitionSort.restore(currentPartition), blockBuffer.toBytes());
            }
        }

        // add last partition and spill
        if (currentPartition != -1) {
            blockManager.spill(partitionSort.restore(currentPartition), blockBuffer.toBytes());
        }
    }

    /**
     * Sort and spill the current records in response to memory pressure.
     */
    @Override
    public long spill(long size, MemoryConsumer trigger) throws IOException {
        if (trigger != this || inMemSorter == null || inMemSorter.numRecords() == 0) {
            return 0L;
        }
        final long start = System.currentTimeMillis();
        memorySpillCount += 1;
        final long memorySize = getMemoryUsage();

        writeToServer();

        final long spillSize = freeMemory();
        inMemSorter.reset();
        // Reset the in-memory sorter's pointer array only after freeing up the memory pages holding the
        // records. Otherwise, if the task is over allocated memory, then without freeing the memory
        // pages, we might not be able to get memory for the pointer array.
        taskContext.taskMetrics().incMemoryBytesSpilled(spillSize);

        logger.info("Thread {} spilling sort data of {} to ors2-server ({} {} so far), write cost {} ms",
                Thread.currentThread().getId(),
                Utils.bytesToString(memorySize),
                memorySpillCount,
                memorySpillCount > 1 ? " times" : " time",
                System.currentTimeMillis() - start);

        return spillSize;
    }

    private long getMemoryUsage() {
        long totalPageSize = 0;
        for (MemoryBlock page : allocatedPages) {
            totalPageSize += page.size();
        }
        return ((inMemSorter == null) ? 0 : inMemSorter.getMemoryUsage()) + totalPageSize;
    }

    private void updatePeakMemoryUsed() {
        long mem = getMemoryUsage();
        if (mem > peakMemoryUsedBytes) {
            peakMemoryUsedBytes = mem;
        }
    }

    /**
     * Return the peak memory used so far, in bytes.
     */
    long getPeakMemoryUsedBytes() {
        updatePeakMemoryUsed();
        return peakMemoryUsedBytes;
    }

    private long freeMemory() {
        updatePeakMemoryUsed();
        long memoryFreed = 0;
        for (MemoryBlock block : allocatedPages) {
            memoryFreed += block.size();
            freePage(block);
        }
        allocatedPages.clear();
        currentPage = null;
        pageCursor = 0;
        return memoryFreed;
    }

    /**
     * Force all memory and spill files to be deleted; called by shuffle error-handling code.
     */
    public void cleanupResources() {
        freeMemory();
        if (inMemSorter != null) {
            inMemSorter.free();
            inMemSorter = null;

            writeBuffer = null;
        }

        // no SpillInfo
    }

    /**
     * Checks whether there is enough space to insert an additional record in to the sort pointer
     * array and grows the array if additional space is required. If the required space cannot be
     * obtained, then the in-memory data will be spilled to disk.
     */
    private void growPointerArrayIfNecessary() throws IOException {
        assert(inMemSorter != null);
        if (!inMemSorter.hasSpaceForAnotherRecord()) {
            long used = inMemSorter.getMemoryUsage();
            LongArray array;
            try {
                // could trigger spilling
                array = allocateArray(used / 8 * 2);
            } catch (TooLargePageException e) {
                // The pointer array is too big to fix in a single page, spill.
                spill();
                return;
            } catch (SparkOutOfMemoryError e) {
                // should have trigger spilling
                if (!inMemSorter.hasSpaceForAnotherRecord()) {
                    logger.error("Unable to grow the pointer array");
                    throw e;
                }
                return;
            }
            // check if spilling is triggered or not
            if (inMemSorter.hasSpaceForAnotherRecord()) {
                freeArray(array);
            } else {
                inMemSorter.expandPointerArray(array);
            }
        }
    }

    /**
     * Allocates more memory in order to insert an additional record. This will request additional
     * memory from the memory manager and spill if the requested memory can not be obtained.
     *
     * @param required the required space in the data page, in bytes, including space for storing
     *                      the record size. This must be less than or equal to the page size (records
     *                      that exceed the page size are handled via a different code path which uses
     *                      special overflow pages).
     */
    private void acquireNewPageIfNecessary(int required) {
        if (currentPage == null ||
                pageCursor + required > currentPage.getBaseOffset() + currentPage.size() ) {
            // TODO: try to find space in previous pages
            currentPage = allocatePage(required);
            pageCursor = currentPage.getBaseOffset();
            allocatedPages.add(currentPage);
        }
    }

    /**
     * Write a record to the shuffle sorter.
     */
    public void insertRecord(Object recordBase, long recordOffset, int length, int partitionId)
            throws IOException {

        // for tests
        assert(inMemSorter != null);
        if (inMemSorter.numRecords() >= numElementsForSpillThreshold) {
            logger.info("Spilling data because number of spilledRecords crossed the threshold " +
                    numElementsForSpillThreshold);
            spill();
        }

        if (recordUsedMemory>= memoryForSpillThreshold) {
            spill();
            recordUsedMemory = 0;
        }

        growPointerArrayIfNecessary();
        final int uaoSize = UnsafeAlignedOffset.getUaoSize();
        // Need 4 or 8 bytes to store the record length.
        final int required = length + uaoSize;
        acquireNewPageIfNecessary(required);

        assert(currentPage != null);
        final Object base = currentPage.getBaseObject();
        final long recordAddress = taskMemoryManager.encodePageNumberAndOffset(currentPage, pageCursor);
        UnsafeAlignedOffset.putSize(base, pageCursor, length);
        pageCursor += uaoSize;
        Platform.copyMemory(recordBase, recordOffset, base, pageCursor, length);
        pageCursor += length;
        recordUsedMemory += length;
        inMemSorter.insertRecord(recordAddress, partitionSort.sort(partitionId));
    }

    public MapStatus closeAndGetMapStatus() {
        if (inMemSorter != null) {
            // Do not count the final file towards the spill count.
            writeToServer();
            blockManager.spillEnd();
            freeMemory();
            inMemSorter.free();
            inMemSorter = null;

            blockBuffer.close();
            blockManager.closeChannel();
        }
        return blockManager.getMapStatus();
    }
}
