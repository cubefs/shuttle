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

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.ors2.{Ors2BlockManager, Ors2ExternalSorter}

case class Ors2SortShuffleWriter[K, V, C](
   blockManager: Ors2BlockManager,
   memoryManager: TaskMemoryManager,
   dep: ShuffleDependency[K, V, C],
   taskContext: TaskContext,
   sparkConf: SparkConf)
  extends ShuffleWriter[K, V] with Logging {

  private var sorter: Ors2ExternalSorter[K, V, C] = _

  private var stopping = false

  private var mapStatus: MapStatus = _

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    sorter = if (dep.mapSideCombine) {
      Ors2ExternalSorter(
        blockManager, taskContext, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
    } else {
      // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
      // care whether the keys get sorted in each partition; that will be done on the reduce side
      // if the operation being run is sortByKey.
      Ors2ExternalSorter(
        blockManager, taskContext, None, Some(dep.partitioner), None, dep.serializer)
    }

    sorter.insertAll(records)
    mapStatus = sorter.closeAndGetMapStatus()
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        Option(mapStatus)
      } else {
        None
      }
    } finally {
      // Clean up our sorter, which may have its own intermediate files
      if (sorter != null) {
        sorter.stop()
        sorter = null
      }
    }
  }
}
