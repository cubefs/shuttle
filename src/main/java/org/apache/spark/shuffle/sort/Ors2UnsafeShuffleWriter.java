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

import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.shuffle.ors2.Ors2BlockManager;
import org.apache.spark.unsafe.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Option;
import scala.Product2;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;

/**
 * Most commonly used in shuffle scene
 *
 * @see UnsafeShuffleWriter
 */
public class Ors2UnsafeShuffleWriter<K, V, C> extends ShuffleWriter<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(Ors2UnsafeShuffleWriter.class);

    private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();

    @VisibleForTesting
    static final int DEFAULT_INITIAL_SORT_BUFFER_SIZE = 4096;
    static final int DEFAULT_INITIAL_SER_BUFFER_SIZE = 1024 * 1024;

    private final Ors2BlockManager blockManager;
    private final TaskMemoryManager memoryManager;
    private final ShuffleDependency<K, V, C> shuffleDependency;
    private final SerializerInstance serializer;
    private final Partitioner partitioner;
    private final TaskContext taskContext;
    private final SparkConf sparkConf;
    private final int initialSortBufferSize;

    @Nullable private MapStatus mapStatus;
    @Nullable private Ors2ShuffleExternalSorter sorter;
    private long peakMemoryUsedBytes = 0;

    /** Subclass of ByteArrayOutputStream that exposes `buf` directly. */
    static final class MyByteArrayOutputStream extends ByteArrayOutputStream {
        MyByteArrayOutputStream(int size) { super(size); }
        public byte[] getBuf() { return buf; }
    }

    private MyByteArrayOutputStream serBuffer;
    private SerializationStream serOutputStream;

    /**
     * Are we in the process of stopping? Because map tasks can call stop() with success = true
     * and then call stop() with success = false if they get an exception, we want to make sure
     * we don't try deleting files, etc twice.
     */
    private boolean stopping = false;

    public Ors2UnsafeShuffleWriter(
            Ors2BlockManager blockManager,
            TaskMemoryManager memoryManager,
            ShuffleDependency<K, V, C> dep,
            TaskContext taskContext,
            SparkConf sparkConf) {
        final int numPartitions = dep.partitioner().numPartitions();
        if (numPartitions > SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE()) {
            throw new IllegalArgumentException(
                    "UnsafeShuffleWriter can only be used for shuffles with at most " +
                            SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE() +
                            " reduce partitions");
        }
        this.blockManager = blockManager;
        this.memoryManager = memoryManager;
        this.shuffleDependency = dep;
        this.serializer = dep.serializer().newInstance();
        this.partitioner = dep.partitioner();
        this.taskContext = taskContext;
        this.sparkConf = sparkConf;
        this.initialSortBufferSize = sparkConf.getInt("spark.shuffle.sort.initialBufferSize",
                DEFAULT_INITIAL_SORT_BUFFER_SIZE);
        open();
    }

    private void updatePeakMemoryUsed() {
        // sorter can be null if this writer is closed
        if (sorter != null) {
            long mem = sorter.getPeakMemoryUsedBytes();
            if (mem > peakMemoryUsedBytes) {
                peakMemoryUsedBytes = mem;
            }
        }
    }

    /**
     * Return the peak memory used so far, in bytes.
     */
    public long getPeakMemoryUsedBytes() {
        updatePeakMemoryUsed();
        return peakMemoryUsedBytes;
    }

    /**
     * This convenience method should only be called in test code.
     */
    @VisibleForTesting
    public void write(Iterator<Product2<K, V>> records) throws IOException {
        write(JavaConverters.asScalaIteratorConverter(records).asScala());
    }

    @Override
    public void write( scala.collection.Iterator<Product2<K, V>> records ) throws IOException {
        // Keep track of success so we know if we encountered an exception
        // We do this rather than a standard try/catch/re-throw to handle
        // generic throwables.
        boolean success = false;
        try {
            long start = System.currentTimeMillis();
            executeSync( records );
            logger.info( "Finish to insert record, cost {} ms", System.currentTimeMillis() - start);
            closeAndWriteOutput();
            success = true;
        }
        finally {
            if ( sorter != null ) {
                try {
                    sorter.cleanupResources();
                }
                catch ( Exception e ) {
                    // Only throw this error if we won't be masking another error.
                    if ( success ) {
                        throw e;
                    }
                    else {
                        logger.error( "In addition to a failure during writing, we failed during " + "cleanup.", e );
                    }
                }
            }
        }
    }

    private void executeSync( scala.collection.Iterator<Product2<K, V>> records ) throws IOException {
        while ( records.hasNext() ) {
            insertRecordIntoSorter( records.next() );
        }
    }

    private void open() {
        assert (sorter == null);
        sorter = new Ors2ShuffleExternalSorter(
                blockManager,
                memoryManager,
                taskContext,
                initialSortBufferSize,
                partitioner.numPartitions(),
                sparkConf);
        serBuffer = new MyByteArrayOutputStream(DEFAULT_INITIAL_SER_BUFFER_SIZE);
        serOutputStream = serializer.serializeStream(serBuffer);
    }

    @VisibleForTesting
    void closeAndWriteOutput() {
        assert(sorter != null);
        updatePeakMemoryUsed();
        serBuffer = null;
        serOutputStream = null;
        mapStatus = sorter.closeAndGetMapStatus();
        sorter = null;
    }

    @VisibleForTesting
    void insertRecordIntoSorter(Product2<K, V> record) throws IOException {
        assert(sorter != null);
        final K key = record._1();
        final int partitionId = partitioner.getPartition(key);

        Object value;
        // support combiner
        if (shuffleDependency.mapSideCombine()) {
            Function1<V, C> createCombiner = shuffleDependency.aggregator().get().createCombiner();
            value = createCombiner.apply(record._2());
        } else {
            value = record._2();
        }

        serBuffer.reset();
        serOutputStream.writeKey(key, OBJECT_CLASS_TAG);
        serOutputStream.writeValue(value, OBJECT_CLASS_TAG);
        serOutputStream.flush();

        final int serializedRecordSize = serBuffer.size();
        assert (serializedRecordSize > 0);

        sorter.insertRecord(
                serBuffer.getBuf(), Platform.BYTE_ARRAY_OFFSET, serializedRecordSize, partitionId);
    }

    @VisibleForTesting
    void forceSorterToSpill() throws IOException {
        assert (sorter != null);
        sorter.spill();
    }

    @Override
    public Option<MapStatus> stop(boolean success) {
        try {
            taskContext.taskMetrics().incPeakExecutionMemory(getPeakMemoryUsedBytes());

            if (stopping) {
                return Option.apply(null);
            } else {
                stopping = true;
                if (success) {
                    if (mapStatus == null) {
                        throw new IllegalStateException("Cannot call stop(true) without having called write()");
                    }
                    return Option.apply(mapStatus);
                } else {
                    return Option.apply(null);
                }
            }
        } finally {
            if (sorter != null) {
                // If sorter is non-null, then this implies that we called stop() in response to an error,
                // so we need to clean up memory and spill files created by the sorter
                sorter.cleanupResources();
            }
        }
    }
}
