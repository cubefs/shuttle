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

import org.apache.spark.serializer.SerializerInstance

import scala.collection.mutable

case class Ors2ShuffleWorkerDataBuffer(blockManager: Ors2BlockManager, ser: SerializerInstance,
                                       compression: Ors2Compression, workerId: Int, writeBlockSize: Int,
                                       writerBufferSpill: Long){
  private var totalDataSize = 0L

  // partitionId->dataBuffer
  val partitionDataBuffer = mutable.Map[Integer, Ors2BlockBuffer]()

  private def getPartitionDataBuffer(partitionId: Int): Ors2BlockBuffer ={
    if (!partitionDataBuffer.contains(partitionId)) {
      val blockBuffer = blockManager.getBlockBuffer(ser, 256 * 1024, writerBufferSpill.toInt)
      partitionDataBuffer.put(partitionId, blockBuffer)
    }
    partitionDataBuffer(partitionId)
  }

  def write(partitionId: Integer, elm: Product2[Any, Any]): Long = {
    var deltaSize = 0L
    val blockBuffer = getPartitionDataBuffer(partitionId)
    val beforeOffset = blockBuffer.position
    blockBuffer.write(elm)
    val afterOffset = blockBuffer.position
    deltaSize = afterOffset - beforeOffset
    totalDataSize += deltaSize
    if (totalDataSize > writeBlockSize) {
      blockManager.sendSingleWorkerData(partitionDataBuffer, workerId,false)
      deltaSize = (afterOffset - beforeOffset) - totalDataSize
      totalDataSize = 0
    }
    deltaSize
  }

  def write(partitionId: Integer, data: Array[Byte], offset: Int, length: Int): Long = {
    var deltaSize = 0L
    val blockBuffer = getPartitionDataBuffer(partitionId)
    val beforeOffset = blockBuffer.position
    blockBuffer.write(data, offset, length)
    val afterOffset = blockBuffer.position
    deltaSize = afterOffset - beforeOffset
    totalDataSize += deltaSize
    if (totalDataSize > writeBlockSize) {
      blockManager.sendSingleWorkerData(partitionDataBuffer, workerId,false)
      deltaSize = (afterOffset - beforeOffset) - totalDataSize
      totalDataSize = 0
    }
    deltaSize
  }

  def close(): Unit = {
    partitionDataBuffer.foreach(_._2.close())
  }
}
