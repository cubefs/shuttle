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

import com.oppo.shuttle.rss.storage.ShuffleFileStorage
import com.oppo.shuttle.rss.storage.fs.{FileSystem, Path}
import com.oppo.shuttle.rss.storage.fs.cfs.CfsFileSystem
import com.oppo.shuttle.rss.storage.fs.dfs.DfsFileSystem
import com.oppo.shuttle.rss.storage.fs.local.LocalFileSystem
import org.slf4j.LoggerFactory

/**
 * Some storage systems only support windows, and all test cases are placed here.
 */
class FileSystemTestUtil(uri: String) {
  private val logger = LoggerFactory.getLogger(classOf[FileSystemTestUtil])

  val storage = new ShuffleFileStorage(uri)

  val rootDir = storage.getRootDir

  val fs = storage.getFs

  def createPath(path: String): Path = {
    Path.of(rootDir, path)
  }

  def createFileSystem(): Unit = {
    val local = FileSystem.get("a/b/c")
    val dfs = FileSystem.get("hdfs:///x/y/z")
    val dfs1 = FileSystem.get("hdfs:///x/y/z")
    val cfs = FileSystem.get("cfs://")

    assert(dfs eq dfs1)
    assert(local.isInstanceOf[LocalFileSystem])
    assert(dfs.isInstanceOf[DfsFileSystem])
    assert(cfs.isInstanceOf[CfsFileSystem])
  }

  def createDir(): Unit = {
    val path = createPath("/a/b/c")
    fs.mkdirs(path)
    assert(fs.exists(path))

    logger.info("createDir success")
  }

  def readAndWrite(): Unit = {
    val path = createPath("/a/b/c.log")
    val outputStream = fs.create(path, true)
    outputStream.write("123".getBytes)
    outputStream.close()

    val bytes = new Array[Byte](3)
    val inputStream = fs.open(path)
    inputStream.read(bytes)
    assert(new String(bytes) == "123")
    inputStream.close()

    logger.info("readAndWrite success")
  }

  def rename(): Unit = {
    val src = createPath("a/b/c.log")
    fs.create(src, true).close()
    val dst = createPath("a/y/z/f.log")

    System.out.println(fs.rename(src, dst))
    assert(!fs.exists(src))
    assert(fs.exists(dst))

    logger.info("rename success")
  }

  def status(): Unit = {
    val path = createPath("a.log")
    val outputStream = fs.create(path, true)
    outputStream.write("123".getBytes)
    assert(outputStream.getPos == 3)
    outputStream.close()

    val status = fs.getFileStatus(path)
    assert(status.getLen == 3)

    logger.info("status success")
  }

  def seekAndSkip(): Unit = {
    val path = createPath("seek.log")
    val outputStream = fs.create(path, true)
    outputStream.write("abcd123456".getBytes())
    outputStream.close()

    val inputStream = fs.open(path)
    inputStream.seek(1)
    val seekBytes = inputStream.readFully(3)
    assert(new String(seekBytes) == "bcd")

    inputStream.skip(3)
    val skipBytes = inputStream.readFully(3)
    assert(new String(skipBytes) == "456")

    inputStream.close()

    logger.info("seekAndSkip success")
  }

  def listFiles(): Unit = {
    val file1 = createPath("list/a.log")
    fs.create(file1, true).close()

    val file2 = createPath("list/b.log")
    fs.create(file2, true).close()

    val file3 = createPath("list/dir/c.log")
    fs.create(file3, true).close()

    import scala.collection.JavaConverters._
    val statuses = fs.listAllFiles(createPath("list")).asScala
    statuses.foreach(println)

    assert(statuses.size == 3)
    assert(statuses.exists(x => x.getPath.getName == "a.log"))
    assert(statuses.exists(x => x.getPath.getName == "b.log"))
    assert(statuses.exists(x => x.getPath.getName == "c.log"))

    logger.info("listFiles success")
  }

  def delete(): Unit = {
    println(fs.delete(Path.of(rootDir), true))

    !fs.exists(Path.of(rootDir))

    logger.info("delete success")
  }

  def run(deleteDir: Boolean): Unit = {
    createDir()
    readAndWrite()
    rename()
    status()
    seekAndSkip()
    listFiles()

    if (deleteDir) delete()
  }
}

object FileSystemTestUtil {
  def main(args: Array[String]): Unit = {
    val uri = args(0)
    new FileSystemTestUtil(uri).run(true)
  }
}
