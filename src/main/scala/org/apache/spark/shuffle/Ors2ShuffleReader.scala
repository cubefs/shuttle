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

package org.apache.spark.shuffle

import com.oppo.shuttle.rss.common.StageShuffleId
import org.apache.spark.executor.ShuffleReadMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ors2.{Ors2ClusterConf, ShuffleMultiReaderRecordIterator}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark.{InterruptibleIterator, ShuffleDependency, SparkConf, TaskContext}

class Ors2ShuffleReader[K, C](
  user: String,
  clusterConf: Ors2ClusterConf,
  stageShuffleId: StageShuffleId,
  startMapIndex: Int,
  endMapIndex: Int,
  startPartition: Int,
  endPartition: Int,
  serializer: Serializer,
  context: TaskContext,
  conf: SparkConf,
  shuffleDependency: ShuffleDependency[K, _, C],
  inputReadyCheckInterval: Long,
  inputReadyWaitTime: Long,
  shuffleMetrics: ShuffleReadMetrics
) extends ShuffleReader[K, C] with Logging {
  override def read(): Iterator[Product2[K, C]] = {
    val readType = conf.get(Ors2Config.shuffleReadType)
    logInfo(s"shuttle rss read started, readType: $readType, appShuffleId: $stageShuffleId, partitions: [$startPartition, $endPartition)")

    val shuffleDep = shuffleDependency
    logInfo(s"Shuffle aggregatorDefined? ${shuffleDep.aggregator.isDefined}, " +
      s"mapSideCombine? ${shuffleDep.mapSideCombine}, keyOrdering? ${shuffleDep.keyOrdering}")

    val readerRecordIterator =  new ShuffleMultiReaderRecordIterator(
      user = user,
      clusterConf,
      stageShuffleId,
      startMapIndex = startMapIndex,
      endMapIndex = endMapIndex,
      startPartition = startPartition,
      endPartition = endPartition,
      serializer = serializer,
      inputReadyCheckInterval = inputReadyCheckInterval,
      inputReadyWaitTime = inputReadyWaitTime,
      context = context,
      conf = conf,
      shuffleReadMetrics = shuffleMetrics
    )

    val aggregatorIter: Iterator[Product2[K, C]] = if (shuffleDep.aggregator.isDefined) {
      if (shuffleDep.mapSideCombine) {
        // Reading combined records
        shuffleDep.aggregator.get.combineCombinersByKey(readerRecordIterator, context)
      } else {
        // Combine read records, note: make sure compatible with shuffleDep aggregator
        val keyValuesIterator = readerRecordIterator.asInstanceOf[Iterator[(K, Nothing)]]
        shuffleDep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      require(!shuffleDep.mapSideCombine, "Map side enabled combine, but no aggregator is defined!")
      readerRecordIterator
    }

    // TODO: sorting optimize: https://doc.myoas.com/pages/viewpage.action?pageId=368664677
    val resultIter = shuffleDep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Create an ExternalSorter to sort the data
        val sorter = new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = shuffleDep.serializer)
        logInfo(s"Inserting aggregated records to sorter: $stageShuffleId")
        val startTime = System.currentTimeMillis()
        sorter.insertAll(aggregatorIter)
        logInfo(s"Inserted aggregated records to sorter: $stageShuffleId, partition [$startPartition, $endPartition)," +
          s" millis: ${System.currentTimeMillis() - startTime}")
        context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
        context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
        // Use completion callback to stop sorter if task was finished/cancelled.
        context.addTaskCompletionListener[Unit](_ => {
          sorter.stop()
        })
        CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
      case None =>
        aggregatorIter
    }

    resultIter match {
      case _: InterruptibleIterator[Product2[K, C]] => resultIter
      case _ =>
        // Use another interruptible iterator here to support task cancellation as aggregator
        // or(and) sorter may have consumed previous interruptible iterator.
        new InterruptibleIterator[Product2[K, C]](context, resultIter)
    }
  }
}
