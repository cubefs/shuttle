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

package org.apache.spark.shuffle

import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.ors2._
import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}

/**
 * Used when partition num under 200 or operator type not like reduceByKey
 *
 * @param blockManager
 * @param memoryManager
 * @param shuffleDependency
 * @param taskContext
 * @param sparkConf
 */
case class Ors2BypassShuffleWriter[K, V, C](
   blockManager: Ors2BlockManager,
   memoryManager: TaskMemoryManager,
   shuffleDependency: ShuffleDependency[K, V, C],
   taskContext: TaskContext,
   sparkConf: SparkConf)
  extends ShuffleWriter[K, V] with Logging {

  private val numPartitions = shuffleDependency.partitioner.numPartitions
  private var mapStatus: MapStatus = _

  private var stopping = false

  private def getPartition(key: K): Int = {
    if (numPartitions > 1) {
      shuffleDependency.partitioner.getPartition(key)
    } else {
      0
    }
  }

  private val bufferManager = Ors2PartitionWriter[K, V, C](
    blockManager, shuffleDependency, sparkConf
   )

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    while (records.hasNext) {
      val record = records.next()
      val partition = getPartition(record._1)

      if (shuffleDependency.mapSideCombine) {
        val createCombiner = shuffleDependency.aggregator.get.createCombiner
        val c = createCombiner(record._2)
        bufferManager.addRecord(partition, (record._1, c))
      } else {
        bufferManager.addRecord(partition, record)
      }
    }

    mapStatus = bufferManager.closeAndGetMapStatus()
  }


  override def stop(success: Boolean): Option[MapStatus] = {
    if (stopping) {
      return None
    }

    stopping = true
    if (success) {
      Option(mapStatus)
    } else {
      None
    }
  }
}
