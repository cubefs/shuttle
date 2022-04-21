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

import java.util
import java.util.concurrent.TimeUnit
import com.oppo.shuttle.rss.clients.{NettyClient, Ors2ClientFactory}
import com.oppo.shuttle.rss.common.{AppTaskInfo, Constants, Ors2ServerGroup}
import com.oppo.shuttle.rss.messages.ShuffleMessage.UploadPackageRequest.PartitionBlockData
import com.oppo.shuttle.rss.messages.ShuffleMessage.{BuildConnectionRequest, UploadPackageRequest}
import com.oppo.shuttle.rss.messages.ShufflePacket
import com.oppo.shuttle.rss.util.ChecksumUtils
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.shuffle.Ors2Config
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkEnv, TaskContext}

import scala.collection.JavaConverters._
import scala.collection.mutable

case class Ors2BlockManager(
  taskContext: TaskContext,
  numPartitions: Int,
  partitionMapToShuffleWorkers: Map[Int, Int],
  appTaskInfo: AppTaskInfo,
  ors2Servers: List[Ors2ServerGroup],
  writeMetrics: ShuffleWriteMetrics,
  maxBufferSize: Int,
  clientFactory: Ors2ClientFactory
) extends Logging {
  private val conf = if (taskContext != null) SparkEnv.get.conf else new SparkConf()
  val writeBlockSize: Int = conf.get(Ors2Config.writeBlockSize).toInt
  val maxFlyingPackageNum: Int = conf.get(Ors2Config.maxFlyingPackageNum)
  val writeMaxRequestSize: Long =  conf.get(Ors2Config.writerMaxRequestSize)
  private var serInstance: SerializerInstance = _

  private val consumer = if (taskContext != null){{
    new Ors2MemoryConsumer(taskContext.taskMemoryManager())
  }} else {
    null
  }
  if (consumer != null)  {
    consumer.acquireMemory(Ors2Config.writerBufferSpill.key, maxBufferSize)
  }

  logInfo(s"Ors2BlockManager: blockSize $writeBlockSize, " +
    s"writerMaxRequestSize $writeMaxRequestSize, " +
    s"maxFlyingPackageNum $maxFlyingPackageNum, " +
    s"maxBufferSize $maxBufferSize")

  def setSerInstance(ser: SerializerInstance): Unit = {
    serInstance = ser
  }

  // workerId->List[partitionDataBuffer]
  @volatile private var workerPartitionDataBuffer = mutable.Map[Integer, Ors2ShuffleWorkerDataBuffer]()

  private val checksums = Array.fill(numPartitions)(Constants.EMPTY_CHECKSUM_DEFAULT)

  private val _partitionLengths: Array[Long]  = Array.fill(numPartitions)(0L)
  def partitionLengths: Array[Long] = _partitionLengths

  private var _spillCount = 0
  private var _spillTime = 0L
  private var _spillDataSize = 0L

  def recordsAdd(): Unit = writeMetrics.incRecordsWritten(1)

  def spillCount(): Long = _spillCount

  // Network request auto-increment ID
  private var _seqId = -1

  private def seqId(): Int = {
    _seqId += 1
    _seqId
  }

  // Data block request self-increase Id, a request may contain multiple blocks
  private var _blockSeqId = -1

  private def blockSeqId(): Int = {
    _blockSeqId += 1
    _blockSeqId
  }

  val nettyClient = new NettyClient(ors2Servers.asJava, conf, appTaskInfo, clientFactory)

  val workerToPartition = partitionMapToShuffleWorkers
    .groupBy(_._2) // group by worker id
    .map(x => (x._1, x._2.keys)) // worker_id as key，partitions as value。

  val version: Int = conf.get(Ors2Config.ShuffleVersion)
  val jobPriority: Int = conf.get(Ors2Config.jobPriority)

  def getWorkerDataBuffer(partitionId: Int): Ors2ShuffleWorkerDataBuffer = {
    val workerId = partitionMapToShuffleWorkers(partitionId)
    if (!workerPartitionDataBuffer.contains(workerId)) {
      workerPartitionDataBuffer.put(workerId, new Ors2ShuffleWorkerDataBuffer(this, serInstance,
        compression, workerId, 256 * 1024, maxBufferSize))
    }
    workerPartitionDataBuffer(workerId)
  }

  def addRecord(partitionId: Int, record: Product2[Any, Any]): Long = {
    val workerDataBuffer = getWorkerDataBuffer(partitionId)
    workerDataBuffer.write(partitionId, record)
  }

  def updateChecksum(partitionId: Int, data: Array[Byte]): Unit = {
    checksums(partitionId) = {
      if (checksums(partitionId) == Constants.EMPTY_CHECKSUM_DEFAULT) {
        ChecksumUtils.getCRC32Checksum(data)
      } else {
        checksums(partitionId) +
          ChecksumUtils.getCRC32Checksum(data)
      }
    }
  }

  def sendDataBlock(workerId: Int, data: mutable.Iterable[(Integer, Array[Byte])]): Unit = {
    val (blockDesc, partitionBlocks) = createPartitionBlockData(data)
    val requestId = NettyClient.requestId()
    val dataBuilder = createDataBuilder(requestId, blockDesc)

    val packet = ShufflePacket.create(createBuildBuilder(requestId), dataBuilder, partitionBlocks)
    nettyClient.send(workerId, packet)
  }

  def createPartitionBlockData(data: mutable.Iterable[(Integer, Array[Byte])]):(mutable.Iterable[PartitionBlockData], util.ArrayList[Array[Byte]]) = {
    val partitionBlocks = new util.ArrayList[Array[Byte]](data.size)
    val blocksDesc = data.map {elm =>
      partitionBlocks.add(elm._2)
      PartitionBlockData
        .newBuilder()
        .setPartitionId(elm._1)
        .setDataLength(elm._2.length)
        .setSeqId(blockSeqId())
        .build()
    }
    (blocksDesc, partitionBlocks)
  }

  def createDataBuilder(messageId: String, partitionBlockData: mutable.Iterable[PartitionBlockData]): UploadPackageRequest.Builder = {
    val numberId = seqId()
    if (partitionBlockData == null || partitionBlockData.isEmpty) {
      return UploadPackageRequest
        .newBuilder()
        .setAppId(appTaskInfo.getAppId)
        .setAppAttempt(appTaskInfo.getAppAttempt)
        .setShuffleId(appTaskInfo.getShuffleId)
        .setMapId(appTaskInfo.getMapId)
        .setAttemptId(appTaskInfo.getTaskAttemptId)
        .setNumMaps(0)
        .setNumPartitions(numPartitions)
        .setCreateTime(System.currentTimeMillis())
        .setMessageId(messageId)
        .setSeqId(numberId)
        .setStageAttempt(appTaskInfo.getStageAttempt)
    }

    val uploadPackageRequestBuilder = UploadPackageRequest
      .newBuilder()
      .addAllPartitionBlocks(partitionBlockData.asJava)
      .setAppId(appTaskInfo.getAppId)
      .setAppAttempt(appTaskInfo.getAppAttempt)
      .setShuffleId(appTaskInfo.getShuffleId)
      .setMapId(appTaskInfo.getMapId)
      .setAttemptId(appTaskInfo.getTaskAttemptId)
      .setNumMaps(0)
      .setNumPartitions(numPartitions)
      .setCreateTime(System.currentTimeMillis())
      .setMessageId(messageId)
      .setSeqId(numberId)
      .setStageAttempt(appTaskInfo.getStageAttempt)

    uploadPackageRequestBuilder
  }

  def createBuildBuilder(messageId: String): BuildConnectionRequest.Builder = {
    BuildConnectionRequest
      .newBuilder
      .setVersion(version)
      .setRetryIdx(0)
      .setJobPriority(jobPriority)
      .setCreateTime(System.currentTimeMillis())
      .setMessageId(messageId)
  }

  def sendCheckSum(): Unit = {
    workerToPartition.foreach(item => {
      val worker = item._1
      val partitions = item._2

      val requestId = NettyClient.requestId()
      val dataBuilder = createDataBuilder(requestId, null)

      val sumPartitions = new util.ArrayList[java.lang.Integer](partitions.size)
      val allChecksums = new util.ArrayList[java.lang.Long](partitions.size)
      partitions.foreach(p => {
        sumPartitions.add(p)
        allChecksums.add(checksums(p))
      })

      val checkSumPackage = UploadPackageRequest.CheckSums
        .newBuilder()
        .addAllChecksumPartitions(sumPartitions)
        .addAllChecksums(allChecksums)

      dataBuilder.setCheckSums(checkSumPackage)
      val packet = ShufflePacket.create(createBuildBuilder(requestId), dataBuilder, util.Collections.emptyList())
      nettyClient.send(worker, packet)
    })
  }

  def getFinishPackageNum: Int = {
    nettyClient.getFinishPackageNum
  }

  val compression = new Ors2Compression(conf)

  def getBlockBuffer(ser: SerializerInstance, blockSize: Int, maxBufferSize: Int): Ors2BlockBuffer = {
    Ors2BlockBuffer(ser, compression, blockSize, maxBufferSize)
  }

  // send single worker data to shuffle worker
  def sendSingleWorkerData(workerDataBuffer: mutable.Map[Integer, Ors2BlockBuffer], workerId: Int, isLast: Boolean): Unit = {
    val start = System.nanoTime()
    var spillBytes = 0L

    val workerDataBlocks = workerDataBuffer.map { item =>
      val bytes = item._2.toBytes
      _partitionLengths(item._1) += bytes.length
      updateChecksum(item._1, bytes)
      spillBytes += bytes.length
      (item._1, bytes)
    }.filter(_._2.length > 0)

    if (workerDataBlocks.isEmpty && !isLast) return

    val (blockDesc, partitionBlocks) = createPartitionBlockData(workerDataBlocks)
    val requestId = NettyClient.requestId()
    val dataBuilder = createDataBuilder(requestId, blockDesc)

    // set checksum to package
    if (isLast) {
      val partitions = workerToPartition(workerId)
      val sumPartitions = new util.ArrayList[java.lang.Integer](partitions.size)
      val allChecksums = new util.ArrayList[java.lang.Long](partitions.size)
      partitions.foreach(p => {
        sumPartitions.add(p)
        allChecksums.add(checksums(p))
      })

      val checkSumPackage = UploadPackageRequest.CheckSums
        .newBuilder()
        .addAllChecksumPartitions(sumPartitions)
        .addAllChecksums(allChecksums)

      dataBuilder.setCheckSums(checkSumPackage)
    }
    val packet = ShufflePacket.create(createBuildBuilder(requestId), dataBuilder, partitionBlocks)
    nettyClient.send(workerId, packet)

    _spillCount += 1
    writeMetrics.incBytesWritten(spillBytes)
    val spillTime = System.nanoTime() - start
    writeMetrics.incWriteTime(spillTime)
    _spillDataSize += spillBytes
    _spillTime += spillTime
  }

  def sendAllWorkerData(isLast: Boolean): Unit = {
    workerPartitionDataBuffer.foreach(t => sendSingleWorkerData(t._2.partitionDataBuffer, t._1, isLast))
  }

  private var workerData = Ors2WorkerData(this)

  def spill(partition: Integer, blockData: Array[Byte]): Unit = {
    if (blockData.isEmpty) return

    val start = System.nanoTime()
    var spillTime = 0L

    updateChecksum(partition, blockData)
    _partitionLengths(partition) += blockData.length

    workerData.write(partition, blockData)

    spillTime = System.nanoTime() - start
    writeMetrics.incBytesWritten(blockData.length)
    writeMetrics.incWriteTime(spillTime)

    _spillDataSize += blockData.length
    _spillTime += spillTime
    _spillCount = workerData.spillCount
  }

  def spillEnd(): Unit = {
    workerData.clear(true)
  }

  def clearWorkerPartitionBuffer(): Unit = {
    workerPartitionDataBuffer.values.foreach(_.close())
    workerPartitionDataBuffer = null
  }

  def closeChannel(): Unit = {
    val start = System.nanoTime()
    nettyClient.waitFinish()
    val spillTime = System.nanoTime() - start
    _spillTime += spillTime
    writeMetrics.incWriteTime(spillTime)
    logInfo(s"Ors2 write finish, total package ${nettyClient.getFinishPackageNum}, total size ${Utils.bytesToString(_spillDataSize)}, " +
      s"sync spill ${_spillCount} times, cost ${TimeUnit.NANOSECONDS.toMillis(_spillTime)} millis.")
    nettyClient.close()

    workerData = null
    if (consumer != null) {
      consumer.freeMemory(Ors2Config.writerBufferSpill.key)
    }
  }

  def getMapStatus: MapStatus = {
    // @todo  nonZero checksums will fail
    // val nonZeroPartitionLengths = _partitionLengths.map(x => if (x == 0) 1 else x)
    val blockManagerId = Ors2Util.mockMapBlockManagerId(
      appTaskInfo.getMapId,
      appTaskInfo.getTaskAttemptId,
      ors2Servers)
    Ors2Util.createMapStatus(blockManagerId, _partitionLengths, appTaskInfo)
  }
}
