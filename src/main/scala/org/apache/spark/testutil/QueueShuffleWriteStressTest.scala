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

package org.apache.spark.testutil

import com.oppo.shuttle.rss.clients.NettyClient
import com.oppo.shuttle.rss.common.{PartitionShuffleId, StageShuffleId}
import com.oppo.shuttle.rss.execution.{Ors2WorkerPartitionExecutor, ShuffleIndex, ShufflePartitionUnsafeWriter}
import com.oppo.shuttle.rss.metrics.Ors2MetricsConstants
import com.oppo.shuttle.rss.storage.ShuffleFileStorage
import com.oppo.shuttle.rss.storage.fs.Path
import com.oppo.shuttle.rss.util.ShuffleUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.util.Utils

import java.util.concurrent.{ConcurrentHashMap, CountDownLatch}
import scala.util.Random

/**
 * Shuffle write stress test
 */
class QueueShuffleWriteStressTest(val dir: String, val blockSize: String, val totalSize: String) {
  val storage = new ShuffleFileStorage(dir)

  storage.createDirectories(dir)

  val blockBytes: Long = Utils.byteStringAsBytes(blockSize)

  val totalBytes: Long = Utils.byteStringAsBytes(totalSize)

  val appId: String = NettyClient.requestId()

  val appShuffleId = new StageShuffleId(appId, "0", 0, 0)

  val shufflePartitionId = new PartitionShuffleId(appShuffleId, 0)

  val writers: ConcurrentHashMap[Int, ShufflePartitionUnsafeWriter] = new ConcurrentHashMap()

  def getOrCreateWriter(threadId: Int): ShufflePartitionUnsafeWriter = {
    writers.computeIfAbsent(threadId, new java.util.function.Function[Int, ShufflePartitionUnsafeWriter](){
      override def apply(t: Int): ShufflePartitionUnsafeWriter = {
        val path = Path.of(dir, t.toString).getPath
        new ShufflePartitionUnsafeWriter(shufflePartitionId, path, 0, storage)
      }
    })
  }

  def put(executor: Ors2WorkerPartitionExecutor, threadNum: Int, totalWriteBytes: Long): Unit = {
    val start = System.currentTimeMillis()

    val bytes = StringUtils.repeat("A", blockBytes.toInt).getBytes()
    var writeBytes = 0L
    var seqId = 0

    while (writeBytes < totalWriteBytes) {
      // Randomly select a thread for delivery
      val partitionId = Random.nextInt(threadNum)
      val executorId = ShuffleUtils.generateShuffleExecutorIndex(appShuffleId.getAppId, 0L,
        partitionId, executor.getPoolSize)
      writeBytes += bytes.length

      Ors2MetricsConstants.bufferedDataSize.inc(bytes.length + ShuffleIndex.NUM_BYTES)

      val mapId = Random.nextInt(1000)
      val start = System.currentTimeMillis()
      executor.execute(executorId, new Runnable {
        override def run(): Unit = {
          getOrCreateWriter(executorId).writeData(bytes, mapId, 0, seqId)
        }
      })
      val cost = System.currentTimeMillis() - start
      if (cost >= 50) {
        println(s"ttt-put $cost ms")
      }


      seqId += 1
    }

    val end = System.currentTimeMillis()
    println(s"${Thread.currentThread().getId}, put ${Utils.bytesToString(writeBytes)}(block $blockSize),  cost: ${end - start} ms")
  }

  def writeMulti(writeThread: Int, workerThread: Int): Unit = {
    val latch = new CountDownLatch(workerThread)
    val executor = new Ors2WorkerPartitionExecutor(writeThread, 100, 0)
    val writeBytes = totalBytes * writeThread / workerThread

    // 50 delivery threads, which is equivalent to the number of threads in netty IO
    0.until(workerThread).foreach(i => {
      new Thread() {
        override def run(): Unit = {
          // Decentralized delivery of data
          put(executor, writeThread, writeBytes)
          latch.countDown()
        }
      }.start()
    })

    latch.await()

    // execute finalize
    0.until(writeThread).foreach(i => {
      executor.execute(i, new Runnable {
        override def run(): Unit = {
          getOrCreateWriter(i).finalizeDataAndIndex()
        }
      })
    })

    executor.shutdown()
  }
}


object QueueShuffleWriteStressTest {
  /**
   * example: ors2-data/fs-stress 2 1mb 1gb  1
   */
  def main(args: Array[String]): Unit = {
    val dir = if (args.length >= 1) args(0) else "ors2-data/fs-stress"
    val writeThread = if (args.length >= 2) args(1) else "1"
    val blockSize = if (args.length >= 3) args(2) else "1mb"
    val totalSize = if (args.length >= 4) args(3) else "100mb"
    val workerThread = if (args.length >= 5) args(4) else "2"

    val test = new QueueShuffleWriteStressTest(dir, blockSize, totalSize)

    val start = System.currentTimeMillis()
    test.writeMulti(writeThread.toInt, workerThread.toInt)
    val cost = (System.currentTimeMillis() - start) / 1000F

    val base = (test.totalBytes / cost).toLong

    val spreed = Utils.bytesToString(base * writeThread.toLong)
    val width = Utils.bytesToString(base * writeThread.toLong * 8)
    println(s"QueueShuffleWriteStressTest: writeThread $writeThread, workerThread $workerThread, total $totalSize, block $blockSize;" +
      s" cost $cost s, speed $spreed/s, width $width/s")
  }
}

