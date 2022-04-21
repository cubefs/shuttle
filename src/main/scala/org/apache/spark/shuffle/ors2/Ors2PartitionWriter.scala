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

package org.apache.spark.shuffle.ors2

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.Ors2Config
import org.apache.spark.{ShuffleDependency, SparkConf}

case class Ors2PartitionWriter[K, V, C](
   blockManager: Ors2BlockManager,
   shuffleDependency: ShuffleDependency[K, V, C],
   conf: SparkConf) extends Logging {

  val writeBlockSize: Int = conf.get(Ors2Config.writeBlockSize).toInt
  val writerBufferSpill: Long = conf.get(Ors2Config.writerBufferSpill)
  private var totalBytes = 0L
  blockManager.setSerInstance(shuffleDependency.serializer.newInstance())

  def addRecord(partitionId: Integer, record: Product2[Any, Any]): Unit = {
    blockManager.recordsAdd()
    totalBytes += blockManager.addRecord(partitionId, record)

    if (totalBytes >= writerBufferSpill) {
      blockManager.sendAllWorkerData(false)
      totalBytes = 0
    }
  }

  def closeAndGetMapStatus(): MapStatus = {
    totalBytes = 0
    blockManager.sendAllWorkerData(true)
    blockManager.clearWorkerPartitionBuffer()
    blockManager.closeChannel()
    blockManager.getMapStatus
  }
}