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

import com.oppo.shuttle.rss.execution.Ors2WorkerPartitionExecutor
import com.oppo.shuttle.rss.storage.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.spark.util.Utils

import java.io.EOFException
import scala.collection.JavaConversions._
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch}
import java.util.concurrent.atomic.LongAdder
import java.util.function

class FsReadStressTest(val uri: String, val blockSize: String) {
  val path: Path = Path.of(uri)

  val fs: FileSystem = FileSystem.get(path.toUri)

  val dir: String = path.getPath

  val blockBytes: Long = Utils.byteStringAsBytes(blockSize)

  val totalBytes: LongAdder = new LongAdder

  val start = System.currentTimeMillis()
  private val files = fs.listAllFiles(Path.of(dir))
  println("meta cost " + (System.currentTimeMillis() - start) + " ms")

  def writeMulti(executor: Ors2WorkerPartitionExecutor): Unit = {
    val latch = new CountDownLatch(files.size())
    val map = new ConcurrentHashMap[Path, FSDataInputStream]()

    while (latch.getCount != 0) {
      files.indices.foreach(i => {
        executor.execute(i, new Runnable {
          override def run(): Unit = {
            val file = files(i)
            val input = map.computeIfAbsent(file.getPath,  new function.Function[Path, FSDataInputStream] {
              override def apply(t: Path): FSDataInputStream = {
                val input = fs.open(t)
                totalBytes.add(input.available())
                input
              }
            })

            val bytes = new Array[Byte](blockBytes.toInt)
            try {
              input.readFully(bytes)
            } catch {
              case _: EOFException =>
                input.close()
                latch.countDown()
            }
          }
        })
      })
    }

    executor.shutdown()
  }
}


object FsReadStressTest {
  /**
   * example: ors2-data/fs-stress 10 1mb 1000mb
   */
  def main(args: Array[String]): Unit = {
    val dir = if (args.length >= 1) args(0) else "ors2-data/fs-stress"
    val threadNum = if (args.length >= 2) args(1) else "1"
    val blockSize = if (args.length >= 3) args(2) else "1mb"

    val executor = new Ors2WorkerPartitionExecutor(threadNum.toInt, 100, 0);

    val test = new FsReadStressTest(dir, blockSize)

    val start = System.currentTimeMillis()
    test.writeMulti(executor)
    val cost = (System.currentTimeMillis() - start) / 1000D
    val totalSize = test.totalBytes.longValue()
    val base = (totalSize / cost).toLong

    val spreed = Utils.bytesToString(base)
    val width =  Utils.bytesToString(base * 8)

    val totalSizeString = Utils.bytesToString(totalSize);
    println(s"FsReadStressTest: thread $threadNum,fileNum ${test.files.size}, totalSize $totalSizeString, " +
      s"blockSize $blockSize; cost $cost s, speed $spreed/s, width $width/s")
  }
}
