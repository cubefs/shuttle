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

package org.apache.spark.shuffle.ors2

import com.oppo.shuttle.rss.common.RandomSortPartition

import java.util.Comparator
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer._
import org.apache.spark.shuffle.Ors2Config
import org.apache.spark.util.collection.{PartitionedAppendOnlyMap, PartitionedPairBuffer, Spillable, WritablePartitionedPairCollection}


private[spark] case class Ors2ExternalSorter[K, V, C](
   blockManager: Ors2BlockManager,
   context: TaskContext,
   aggregator: Option[Aggregator[K, V, C]] = None,
   partitioner: Option[Partitioner] = None,
   ordering: Option[Ordering[K]] = None,
   serializer: Serializer = SparkEnv.get.serializer)
  extends Spillable[WritablePartitionedPairCollection[K, C]](context.taskMemoryManager())
    with Logging {

  private val conf = SparkEnv.get.conf

  private val numPartitions = partitioner.map(_.numPartitions).getOrElse(1)
  private val shouldPartition = numPartitions > 1

  val sortPartition = new RandomSortPartition(numPartitions)

  private def getPartition(key: K): Int = {
    val partitionId = if (shouldPartition) partitioner.get.getPartition(key) else 0
    sortPartition.sort(partitionId)
  }

  blockManager.setSerInstance(serializer.newInstance())

  // Data structures to store in-memory objects before we spill. Depending on whether we have an
  // Aggregator set, we either put objects into an AppendOnlyMap where we combine them, or we
  // store them in an array buffer.
  @volatile private var map = new PartitionedAppendOnlyMap[K, C]
  @volatile private var buffer = new PartitionedPairBuffer[K, C]

  // Peak size of the in-memory data structure observed so far, in bytes
  private var _peakMemoryUsedBytes: Long = 0L
  def peakMemoryUsedBytes: Long = _peakMemoryUsedBytes

  val writeBlockSize: Int = conf.get(Ors2Config.writeBlockSize).toInt
  val writerMaxRequestSize: Int = conf.get(Ors2Config.writerMaxRequestSize).toInt
  var totalBufferSize: Long = 0L
  val maxTotalBufferDataSize: Long = conf.get(Ors2Config.maxTotalBufferDataSize).toLong


  // A comparator for keys K that orders them within a partition to allow aggregation or sorting.
  // Can be a partial ordering by hash code if a total ordering is not provided through by the
  // user. (A partial ordering means that equal keys have comparator.compare(k, k) = 0, but some
  // non-equal keys also have this, so we need to do a later pass to find truly equal keys).
  // Note that we ignore this if no aggregator and no ordering are given.
 /* private val keyComparator: Comparator[K] = ordering.getOrElse((a: K, b: K) => {
    val h1 = if (a == null) 0 else a.hashCode()
    val h2 = if (b == null) 0 else b.hashCode()
    if (h1 < h2) -1 else if (h1 == h2) 0 else 1
  })

  private def comparator: Option[Comparator[K]] = {
    if (ordering.isDefined || aggregator.isDefined) {
      Some(keyComparator)
    } else {
      None
    }
  }*/

  def insertAll(records: Iterator[Product2[K, V]]): Unit = {
    // TODO: stop combining if we find that the reduction factor isn't high
    val shouldCombine = aggregator.isDefined

    if (shouldCombine) {
      // Combine values in-memory first using our AppendOnlyMap
      val mergeValue = aggregator.get.mergeValue
      val createCombiner = aggregator.get.createCombiner
      var kv: Product2[K, V] = null
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
      }
      while (records.hasNext) {
        addElementsRead()
        kv = records.next()
        map.changeValue((getPartition(kv._1), kv._1), update)
        maybeSpillCollection(usingMap = true)
      }
    } else {
      // Stick values into our buffer
      while (records.hasNext) {
        addElementsRead()
        val kv = records.next()
        buffer.insert(getPartition(kv._1), kv._1, kv._2.asInstanceOf[C])
        maybeSpillCollection(usingMap = false)
      }
    }
  }

  /**
   * Spill the current in-memory collection to disk if needed.
   *
   * @param usingMap whether we're using a map or buffer as our current in-memory collection
   */
  private def maybeSpillCollection(usingMap: Boolean): Unit = {
    var estimatedSize = 0L
    if (usingMap) {
      estimatedSize = map.estimateSize()
      if (maybeSpill(map, estimatedSize)) {
        map = new PartitionedAppendOnlyMap[K, C]
      }
    } else {
      estimatedSize = buffer.estimateSize()
      if (maybeSpill(buffer, estimatedSize)) {
        buffer = new PartitionedPairBuffer[K, C]
      }
    }

    if (estimatedSize > _peakMemoryUsedBytes) {
      _peakMemoryUsedBytes = estimatedSize
    }
  }

  /**
   * Spill our in-memory collection to grouped by shuffle worker data for sending
   *
   * @param collection whichever collection we're using (map or buffer)
   */
  override protected[this] def spill(collection: WritablePartitionedPairCollection[K, C]): Unit = {
    writeToServer(collection)
  }

  def closeAndGetMapStatus(): MapStatus = {
    val collection = if (aggregator.isDefined) map else buffer
    writeToServer(collection)
    blockManager.sendAllWorkerData(true)
    context.taskMetrics().incPeakExecutionMemory(peakMemoryUsedBytes)

    blockManager.closeChannel()
    blockManager.getMapStatus
  }

  private def writeToServer(collection: WritablePartitionedPairCollection[K, C]): Unit = {
    // We do not need to sort by key, but still need to sort by partition
    val iterator = collection.partitionedDestructiveSortedIterator(None)

    while (iterator.hasNext) {
      val ((partition, key), value) = iterator.next()
      val workerDataBuffer = blockManager.getWorkerDataBuffer(sortPartition.restore(partition))
      blockManager.recordsAdd()
      workerDataBuffer.write(sortPartition.restore(partition), (key, value))

      // send all buffered data to shuffle worker
      if (totalBufferSize > maxTotalBufferDataSize) {
        blockManager.sendAllWorkerData(false)
        totalBufferSize = 0
      }
    }
  }

  /**
   * Force to spilling the current in-memory collection to disk to release memory,
   * It will be called by TaskMemoryManager when there is not enough memory for the task.
   */
  override protected[this] def forceSpill(): Boolean = {
    logWarning("Not enough memory, forcibly execute the spill operation")

    val collection = if (aggregator.isDefined) map else buffer
    writeToServer(collection)
    map = new PartitionedAppendOnlyMap[K, C]
    buffer = new PartitionedPairBuffer[K, C]

    true
  }

  def stop(): Unit = {
    if (map != null || buffer != null) {
      map = null // So that the memory can be garbage-collected
      buffer = null // So that the memory can be garbage-collected
      releaseMemory()
    }
  }

  /**
   * Given a stream of ((partition, key), combiner) pairs *assumed to be sorted by partition ID*,
   * group together the pairs for each partition into a sub-iterator.
   *
   * @param data an iterator of elements, assumed to already be sorted by partition ID
   */
  private def groupByPartition(data: Iterator[((Int, K), C)])
  : Iterator[(Int, Iterator[Product2[K, C]])] =
  {
    val buffered = data.buffered
    (0 until numPartitions).iterator.map(p => (p, new IteratorForPartition(p, buffered)))
  }

  /**
   * An iterator that reads only the elements for a given partition ID from an underlying buffered
   * stream, assuming this partition is the next one to be read. Used to make it easier to return
   * partitioned iterators from our in-memory collection.
   */
  private[this] class IteratorForPartition(partitionId: Int, data: BufferedIterator[((Int, K), C)])
    extends Iterator[Product2[K, C]]
  {
    override def hasNext: Boolean = data.hasNext && data.head._1._1 == partitionId

    override def next(): Product2[K, C] = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val elem = data.next()
      (elem._1._2, elem._2)
    }
  }
}
