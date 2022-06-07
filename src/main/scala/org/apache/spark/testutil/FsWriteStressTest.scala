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
import com.oppo.shuttle.rss.storage.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.util.Utils

import java.util.concurrent.ConcurrentHashMap
import java.util.function

class FsWriteStressTest(val dir: String, val blockSize: String, val totalSize: String) {
  val fs: FileSystem = FileSystem.get(Path.of(dir).toUri)

  fs.mkdirs(Path.of(dir))

  val blockBytes: Long = Utils.byteStringAsBytes(blockSize)

  val totalBytes: Long = Utils.byteStringAsBytes(totalSize)

  def clear(): Unit = {
    fs.delete(Path.of(dir), true)
  }

  def writeMulti(executor: Ors2WorkerPartitionExecutor, fileNum: Int): Unit = {
    val loopNum = totalBytes / blockBytes
    val map = new ConcurrentHashMap[Path, FSDataOutputStream]()
    val bytes = StringUtils.repeat("A", blockBytes.toInt).getBytes()

    0.until(loopNum.toInt).foreach(i => {
      0.until(fileNum).foreach(index => {
        executor.execute(index, new Runnable {
          override def run(): Unit = {
            val path = Path.of(dir, index.toString)
            val output = map.computeIfAbsent(path, new function.Function[Path, FSDataOutputStream] {
              override def apply(t: Path): FSDataOutputStream = fs.create(path, true)
            })
            output.write(bytes)
            if (i == loopNum - 1) {
              output.close()
            }
          }
        })
      })
    })

    executor.shutdown()
  }
}

object FsWriteStressTest {
  /**
   * example: ors2-data/fs-stress 10 1mb 1000mb
   */
  def main(args: Array[String]): Unit = {
    val dir = if (args.length >= 1) args(0) else "ors2-data/fs-stress"
    val threadNum = if (args.length >= 2) args(1) else "1"
    val blockSize = if (args.length >= 3) args(2) else "1mb"
    val fileSize = if (args.length >= 4) args(3) else "1gb"
    val fileNum = if (args.length >= 5) args(4) else threadNum
    val clearFile = if (args.length >=6) args(5) else "true"

    val executor = new Ors2WorkerPartitionExecutor(threadNum.toInt, 100, 0);

    val test = new FsWriteStressTest(dir, blockSize, fileSize)

    val start = System.currentTimeMillis()
    test.writeMulti(executor, fileNum.toInt)
    val cost = (System.currentTimeMillis() - start) / 1000D

    val base = (test.totalBytes * fileNum.toLong / cost).toLong

    val spreed = Utils.bytesToString(base)
    val width =  Utils.bytesToString(base * 8)
    println(s"FsWriteStressTest: thread $threadNum, fileNum $fileNum, fileSize $fileSize, " +
      s"blockSize $blockSize; cost $cost s, speed $spreed/s, width $width/s")

    if (clearFile.toBoolean) {
      test.clear()
    }
  }
}