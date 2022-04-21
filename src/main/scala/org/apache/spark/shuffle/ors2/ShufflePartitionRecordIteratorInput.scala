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

import com.esotericsoftware.kryo.io.Input
import com.oppo.shuttle.rss.clients.ShufflePartitionReader
import com.oppo.shuttle.rss.common.ShuffleDataBlock
import org.apache.spark.internal.Logging

/**
 * Read all data of a partition.
 * A partition may have multiple files created by different shuffle workers, reading different files sequentially.
 */
class ShufflePartitionRecordIteratorInput[K, C](
  shuffleId: Int,
  partition: Int,
  shuffleReader: ShufflePartitionReader) extends Iterator[Input] with Logging {

  private var _numRemoteBytesRead = 0L
  private var _readBlockTimeMillis = 0L

  private var readingEof = false

  private var currentInput: Input = _

  def readNextShuffleInput(): Input = {
    var dataBlock: ShuffleDataBlock = null
    val readStartTime = System.currentTimeMillis()

    try {
      dataBlock = shuffleReader.readDataBlock()
      while (dataBlock != null &&
        (dataBlock.getData == null || dataBlock.getData.size == 0)) {
        dataBlock = shuffleReader.readDataBlock()
      }
      _readBlockTimeMillis += System.currentTimeMillis() - readStartTime
    } catch {
      case ex: Throwable =>
        shuffleReader.close()
        Ors2Util.throwableFetchFailedException(ex, shuffleId, partition)
      }

    if (dataBlock == null) {
      readingEof = true
      shuffleReader.close()
      logShuffleReadInfo()
      return null
    }

    _numRemoteBytesRead += dataBlock.getLength
    new Input(dataBlock.getData)
  }

  def readEof: Boolean = readingEof

  def getPartition: Int = partition

  def numRemoteBytesRead: Long = _numRemoteBytesRead

  def readBlockTimeMillis: Long = _readBlockTimeMillis

  override def hasNext: Boolean = {
    currentInput = readNextShuffleInput()
    if (currentInput == null && readingEof) {
      false
    } else {
      true
    }
  }

  override def next(): Input = {
    currentInput
  }

  def logShuffleReadInfo(): Unit = {
    log.info(s"partition read finish, shuffle $shuffleId, partition $partition, " +
      s" $numRemoteBytesRead bytes, fetch millis: $readBlockTimeMillis, finished: $readingEof")
  }
}
