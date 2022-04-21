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
import org.apache.spark.executor.ShuffleReadMetrics
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{Serializer, SerializerInstance}
import org.apache.spark.shuffle.Ors2Config
import org.apache.spark.util.Utils

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}
import ShuffleMultiReaderRecordIterator._
import com.oppo.shuttle.rss.clients.ShufflePartitionReader
import com.oppo.shuttle.rss.common.StageShuffleId
import com.oppo.shuttle.rss.exceptions.Ors2Exception

import scala.collection.mutable

/**
 * Read multi partitions data as Iterator
 * To read partitions range: [startPartition, endPartition)
 */
class ShuffleMultiReaderRecordIterator[K, C](
  user: String,
  clusterConf: Ors2ClusterConf,
  stageShuffleId: StageShuffleId,
  startMapIndex: Int,
  endMapIndex: Int,
  startPartition: Int,
  endPartition: Int,
  serializer: Serializer,
  inputReadyCheckInterval: Long,
  inputReadyWaitTime: Long,
  context : TaskContext,
  conf: SparkConf,
  shuffleReadMetrics: ShuffleReadMetrics
) extends Iterator[Product2[K, C]] with Logging {

  val shuffleId: Int = stageShuffleId.getShuffleId

  val taskId: Long = context.taskAttemptId()

  val serializerInstance: SerializerInstance = serializer.newInstance()

  val compression: Ors2Compression = new Ors2Compression(conf)

  private var currentIterator: Iterator[Product2[K, C]] = _

  private val totalPartition = endPartition - startPartition

  private var finishPartition = 0

  private val readThread = Math.min(conf.get(Ors2Config.shuffleReadIOThreads), totalPartition)

  private val readMergeSize = conf.get(Ors2Config.shuffleReadMergeSize).toInt
  private val readMaxSize = conf.get(Ors2Config.shuffleReadMaxSize).toInt
  private val queueSize = Math.max(readMaxSize / readMergeSize, 1)

  private var blockQueue: BlockingQueue[ReadResult] = new LinkedBlockingQueue(queueSize)

  private val memoryConsumer = new Ors2MemoryConsumer(context.taskMemoryManager())
  memoryConsumer.acquireMemory(Ors2Config.shuffleReadMaxSize.key, readMaxSize)

  init()

  private def getPartitionRecordIterator(partition: Int): Iterator[Input] = {
    val mapTaskAttemptOptional = getMapTaskAttemptInfo(startMapIndex, endMapIndex, partition)
    if (mapTaskAttemptOptional.isEmpty) {
      return new Iterator[Input] {
        override def hasNext: Boolean = false
        override def next(): Input = throw new Ors2Exception("Cannot get next element on empty iterator")
      }
    }

    val partitionReader = new ShufflePartitionReader(
      clusterConf,
      stageShuffleId,
      partition,
      startMapIndex,
      endMapIndex,
      inputReadyWaitTime,
      conf.get(Ors2Config.mockReadErrorProbability),
      mapTaskAttemptOptional.get.asJavaMapAttempt()
    )

    new ShufflePartitionRecordIteratorInput(
      shuffleId = shuffleId,
      partition = partition,
      shuffleReader = partitionReader)
  }

  private def getMapTaskAttemptInfo(startMapIndex: Int, endMapIndex: Int, partition: Int): Option[MapAttemptInfo] = {
    logInfo(s"Getting map task attempt from map output tracker, shuffleId $shuffleId, partition $partition")
    val mapTaskAttemptOptional = Ors2Util.getMapTaskAttempt(shuffleId, startMapIndex, endMapIndex, partition,
      inputReadyCheckInterval, inputReadyWaitTime)
    if (mapTaskAttemptOptional.isEmpty) {
      None
    } else {
      mapTaskAttemptOptional
    }
  }

  def createIterator(input: WrappedInput): Iterator[Product2[K, C]] = {
    new Iterator[Product2[K, C]] {
      var current: Iterator[Product2[K, C]] = _

      override def hasNext: Boolean = {
        if (current != null && current.hasNext) {
          return true
        }

        if (input.isEmpty) {
          return false
        }

        current = {
          val shufflePartitionStream = serializerInstance.deserializeStream(compression.compressedInputStream(input.dequeue()))
          shufflePartitionStream.asKeyValueIterator.asInstanceOf[Iterator[Product2[K, C]]]
        }

        hasNext
      }

      override def next(): Product2[K, C] = {
        current.next()
      }
    }

  }

  override def hasNext: Boolean = {
    if (currentIterator != null && currentIterator.hasNext) {
      return true
    }

    if (finishPartition >= totalPartition) {
      logShuffleReadInfo()
      stop()
      return false
    }

    val startFetchWait = System.currentTimeMillis()
    blockQueue.take() match {
      case ReadEnd() => finishPartition += 1

      case ReadSuccess(input) =>
        shuffleReadMetrics.incRemoteBytesRead(input.size)
        shuffleReadMetrics.incRemoteBlocksFetched(1)
        currentIterator = createIterator(input)

      case ReadFailure(e) => throw e
    }

    val stopFetchWait = System.currentTimeMillis()
    shuffleReadMetrics.incFetchWaitTime(stopFetchWait - startFetchWait)

    hasNext
  }

  override def next(): Product2[K, C] = {
    shuffleReadMetrics.incRecordsRead(1)
    currentIterator.next
  }


  def init(): Unit = {
    log.info(s"ShuffleMultiReaderRecordIterator, thread: $readThread, partition number: $totalPartition, " +
      s"queueSize: $queueSize, readMaxSize: ${Utils.bytesToString(readMaxSize)}, readMergeSize: ${Utils.bytesToString(readMergeSize)}")

    val threads = 0.until(readThread).map(i => {
      val t = new RunThread(i,s"Executor task launch worker for task $taskId reader $i")
      t.setDaemon(true)
      t
    })

    threads.foreach(_.start())
  }

  class RunThread(val id: Int, val name: String) extends Thread(name) {
    override def run(): Unit = {
      try {
        run0()
      } catch {
        case e: Throwable =>
          blockQueue.put(ReadFailure(e))
      }
    }

    private[this] def run0(): Unit = {
      val start = System.currentTimeMillis()
      val partitionList = startPartition.until(endPartition)
        .filter(_ % readThread == id)

      partitionList.foreach(partition => {
        val partitionIterator = getPartitionRecordIterator(partition)
        var wrappedInput = WrappedInput(readMergeSize)

        while (partitionIterator.hasNext) {
          wrappedInput.add(partitionIterator.next())
          if (wrappedInput.isFull) {
            blockQueue.put(ReadSuccess(wrappedInput))
            wrappedInput = WrappedInput(readMergeSize)
          }
        }

        if (wrappedInput.size > 0) {
          blockQueue.put(ReadSuccess(wrappedInput))
        }

        blockQueue.put(ReadEnd())
      })
      val cost = System.currentTimeMillis() - start
      log.info(s"thread read finish, costTime: $cost, partition size: ${partitionList.size}")
    }
  }

  def logShuffleReadInfo(): Unit = {
    context.taskMetrics().mergeShuffleReadMetrics()

    val e = context.taskMetrics().shuffleReadMetrics
    log.info("task read finish, bytes: %s, records: %s, block: %s, waitTime: %s".format(
      Utils.bytesToString(e.remoteBytesRead),
      e.recordsRead,
      e.remoteBlocksFetched,
      e.fetchWaitTime))
  }

  def stop(): Unit = {
    if (blockQueue != null) {
      blockQueue = null //So that the memory can be garbage-collected
      memoryConsumer.freeMemory(Ors2Config.shuffleReadMaxSize.key)
    }
  }
}

object ShuffleMultiReaderRecordIterator {
  private sealed trait ReadResult {}

  private case class ReadSuccess(input: WrappedInput) extends ReadResult

  private case class ReadEnd() extends ReadResult

  private case class ReadFailure(e: Throwable) extends ReadResult

  private case class WrappedInput(maxSize: Int) {
    private val queue = mutable.Queue[Input]()
    private var _size = 0

    def add(input: Input): Unit = {
      queue.enqueue(input)
      _size += input.available()
    }

    def isFull: Boolean = _size >= maxSize

    def dequeue(): Input = queue.dequeue()

    def isEmpty: Boolean = queue.isEmpty

    def size: Int = _size
  }
}