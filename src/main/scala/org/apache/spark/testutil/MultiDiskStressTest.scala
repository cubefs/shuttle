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

import com.oppo.shuttle.rss.storage.fs.{FileSystem, Path}
import com.oppo.shuttle.rss.storage.fs.local.LocalFileSystem
import org.apache.commons.lang3.StringUtils
import org.apache.spark.util.Utils

import java.nio.ByteBuffer
import java.util.concurrent.CountDownLatch

class MultiDiskStressTest(val dirList: Array[String], val blockSize: String, val totalSize: String) {
  val fs: FileSystem = LocalFileSystem.getInstance()

  val blockBytes: Long = Utils.byteStringAsBytes(blockSize)

  val totalBytes: Long = Utils.byteStringAsBytes(totalSize)

  def write(path: Path): Unit = {
    val output = fs.create(path, true)
    val start = System.currentTimeMillis()

    val bytes = StringUtils.repeat("A", blockBytes.toInt).getBytes()
    val buffer = ByteBuffer.wrap(bytes)
    var writeBytes = 0L
    while (writeBytes < totalBytes) {
      output.write(buffer)
      writeBytes += bytes.length
      buffer.position(0)
    }

    val end = System.currentTimeMillis()
    println(s"$path, write $totalSize(block $blockSize),  cost: ${end - start} ms")

    output.close()
  }

  def writeMulti(num: Int): Unit = {
    val latch = new CountDownLatch(num)
    0.until(num).foreach(i => {
      val dir = dirList(i % dirList.length)

      new Thread(s"write-$i"){
        override def run(): Unit = {
          write(Path.of(dir, i.toString))
          latch.countDown()
        }
      }.start()
    })
    latch.await()
  }

  def clear(): Unit = {
    dirList.foreach(dir => fs.delete(Path.of(dir), true))
  }
}


object MultiDiskStressTest {
  /**
   * example: ors2-data/t1,ors2-data/t2 2 1mb 100mb
   */
  def main(args: Array[String]): Unit = {
    val dirList = args(0).trim
      .split(",")
      .map(_.trim + "/ors2-fs-stress")

    val threadNum = if (args.length >= 2) args(1) else "1"
    val blockSize = if (args.length >= 3) args(2) else "1mb"
    val totalSize = if (args.length >= 4) args(3) else "100mb"

    val test = new MultiDiskStressTest(dirList, blockSize, totalSize)

    val start = System.currentTimeMillis()
    test.writeMulti(threadNum.toInt)
    val cost = (System.currentTimeMillis() - start) / 1000F

    val base = (test.totalBytes / cost).toLong

    val spreed = Utils.bytesToString(base * threadNum.toLong)
    val width =  Utils.bytesToString(base * threadNum.toLong * 8)
    println(s"MultiDiskStressTest: thread $threadNum, total $totalSize, block $blockSize; cost $cost s, speed $spreed/s, width $width/s")

    test.clear()
  }
}