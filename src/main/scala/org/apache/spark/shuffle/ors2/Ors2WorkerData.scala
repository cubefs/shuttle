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

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class Ors2WorkerData(blockManager: Ors2BlockManager) extends Logging {
  val blockSize = blockManager.writeBlockSize
  val maxRequestSize = blockManager.writeMaxRequestSize
  val maxBufferSize  = blockManager.maxBufferSize

  /**
   * [shuffle-worker-id, list([partitionid, array(record data)])]
   */
  val workerData = mutable.Map[Integer, ListBuffer[(Integer, Array[Byte])]]()

  val workerSize = mutable.Map[Integer, Long]()
  private var totalSize = 0L
  private var _spillCount = 0

  def write(partition: Integer, blockData: Array[Byte]): Unit = {
    val workerId = blockManager.partitionMapToShuffleWorkers(partition)

    if (!workerData.contains(workerId)) {
      workerData.put(workerId, ListBuffer())
      workerSize.put(workerId, 0)
    }

    workerData(workerId).append((partition, blockData))
    workerSize(workerId) += blockData.length
    totalSize += blockData.length

    if (workerSize(workerId) >= maxRequestSize) {
      blockManager.sendDataBlock(workerId, workerData(workerId))

      totalSize -= workerSize(workerId)
      workerData.remove(workerId)
      workerSize.remove(workerId)
      _spillCount += 1
    }

    if (totalSize >= maxBufferSize) {
      clear(false)
    }
  }

  def clear(isLast: Boolean): Unit = {
    workerData.iterator.foreach(item => {
      blockManager.sendDataBlock(item._1, item._2)
      workerData.remove(item._1)
      workerSize.remove(item._1)
    })

    _spillCount += 1
    totalSize = 0

    if (isLast) {
      blockManager.sendCheckSum()
    }
  }

  def spillCount: Int  = _spillCount
}

