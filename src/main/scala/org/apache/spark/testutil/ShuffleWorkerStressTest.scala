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

import com.oppo.shuttle.rss.clients.{NettyClient, Ors2ClientFactory}
import com.oppo.shuttle.rss.common.{AppTaskInfo, Ors2ServerGroup, Ors2WorkerDetail, PartitionShuffleId, StageShuffleId}
import com.oppo.shuttle.rss.exceptions.Ors2Exception
import com.oppo.shuttle.rss.storage.fs.{FileSystem, Path}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.shuffle.Ors2Config
import org.apache.spark.shuffle.ors2.Ors2BlockManager
import org.apache.spark.util.Utils

import java.util
import java.util.concurrent.CountDownLatch
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * Shuffle write stress test
 */
class ShuffleWorkerStressTest(val server: String, val blockSize: String, val totalSize: String, val numPartitions: Int) {
  val blockBytes: Long = Utils.byteStringAsBytes(blockSize)

  val totalBytes: Long = Utils.byteStringAsBytes(totalSize)

  val appId: String = NettyClient.requestId()

  val appShuffleId = new StageShuffleId(appId, "0", 0, 0)

  val shufflePartitionId = new PartitionShuffleId(appShuffleId, 0)

  val conf = new SparkConf()

  val serverGroup = new ListBuffer[Ors2ServerGroup]()
  serverGroup.append(new Ors2ServerGroup(util.Arrays.asList(Ors2WorkerDetail.fromSimpleString(server))))

  val partitionMapToShuffleWorkers: Map[Int, Int] = 0.until(numPartitions).map(i => (i, 0)).toMap

  val baseDir: String = System.getProperty("ors2.test.baseDir")
  val fs: FileSystem = FileSystem.get(Path.of(baseDir).toUri)
  fs.delete(Path.of(baseDir, "test", "0", "0", "0"), true)

  def write(mapId: Int): Unit = {
    val appTaskInfo = new AppTaskInfo(appId, "0", partitionMapToShuffleWorkers.size,
      conf.get(Ors2Config.partitionCountPerShuffleWorker), 0, mapId, 0, 0)

    val blockManager = Ors2BlockManager(
      null,
      numPartitions,
      partitionMapToShuffleWorkers,
      appTaskInfo,
      serverGroup.toList,
      new ShuffleWriteMetrics,
      conf.get(Ors2Config.writerBufferSpill).toInt,
      new Ors2ClientFactory(conf)
    )

    val start = System.currentTimeMillis()
    val bytes = StringUtils.repeat("A", blockBytes.toInt).getBytes()
    var writeBytes = 0L
    while (writeBytes < totalBytes) {
      val partitionId = Random.nextInt(numPartitions)
      blockManager.spill(partitionId, bytes)
      writeBytes += bytes.length
    }

    blockManager.spillEnd()
    blockManager.closeChannel()
    val end = System.currentTimeMillis()
    println(s"mapId: $mapId, write ${Utils.bytesToString(writeBytes)}(block $blockSize),  cost: ${end - start} ms")
  }

  def writeMulti(taskNum: Int): Unit = {
    val latch = new CountDownLatch(taskNum)
    0.until(taskNum).foreach(i => {
      new Thread(s"write-$i"){
        override def run(): Unit = {
          write(i)
          latch.countDown()
        }
      }.start()
    })
    latch.await()
  }

  def createSuccessFile(): Unit = {
    fs.create(Path.of(baseDir, appId, "0", "0", "0", "_SUCCEED"), true).close()
  }
}



object ShuffleWorkerStressTest {
  /**
   * example:
   *  vm options -Dors2.test.baseDir=hdfs://ip-10-52-128-224-dg01/user/hive/ors2-data -DHADOOP_USER_NAME=yarn
   *  program options: ip-10-52-128-10-dg01:19190:19191 2 1mb 100mb 2
   */
  def main(args: Array[String]): Unit = {
    val server = if (args.length >= 1) args(0) else throw new Ors2Exception("Not found server")
    val taskNum = if (args.length >= 2) args(1) else "1"
    val blockSize = if (args.length >= 3) args(2) else "1mb"
    val totalSize = if (args.length >= 4) args(3) else "100mb"
    val partitionNum = if (args.length >= 5) args(4) else "2"

    val test = new ShuffleWorkerStressTest(server, blockSize, totalSize, partitionNum.toInt)
    val start = System.currentTimeMillis()
    test.writeMulti(taskNum.toInt)
    test.createSuccessFile()
    val cost = (System.currentTimeMillis() - start) / 1000F

    val base = (test.totalBytes / cost).toLong
    val spreed = Utils.bytesToString(base * taskNum.toLong)
    val width = Utils.bytesToString(base * taskNum.toLong * 8)
    println(s"ShuffleWorkerStressTest: appId: ${test.appId} taskNum $taskNum, total $totalSize, block $blockSize;" +
      s" cost $cost s, speed $spreed/s, width $width/s")
  }
}
