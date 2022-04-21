/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.ors2

import net.jpountz.lz4.{LZ4BlockInputStream, LZ4BlockOutputStream, LZ4Factory}
import net.jpountz.xxhash.XXHashFactory
import org.apache.spark.SparkConf

import java.io.{InputStream, OutputStream}

/**
 * @todo only support lz4
 */
class Ors2Compression(conf: SparkConf) {
  // SPARK-28102: if the LZ4 JNI libraries fail to initialize then `fastestInstance()` calls fall
  // back to non-JNI implementations but do not remember the fact that JNI failed to load, so
  // repeated calls to `fastestInstance()` will cause performance problems because the JNI load
  // will be repeatedly re-attempted and that path is slow because it throws exceptions from a
  // static synchronized method (causing lock contention). To avoid this problem, we cache the
  // result of the `fastestInstance()` calls ourselves (both factories are thread-safe).
  @transient private[this] lazy val lz4Factory: LZ4Factory = LZ4Factory.fastestInstance()
  @transient private[this] lazy val xxHashFactory: XXHashFactory = XXHashFactory.fastestInstance()

  private[this] val defaultSeed: Int = 0x9747b28c // LZ4BlockOutputStream.DEFAULT_SEED

  def compressedOutputStream(s: OutputStream): OutputStream = {
    val blockSize = conf.getSizeAsBytes("spark.io.compression.lz4.blockSize", "32k").toInt
    val syncFlush = true // change to true。
    new LZ4BlockOutputStream(
      s,
      blockSize,
      lz4Factory.fastCompressor(),
      xxHashFactory.newStreamingHash32(defaultSeed).asChecksum,
      syncFlush)
  }

  def compressedInputStream(s: InputStream): InputStream = {
    val disableConcatenationOfByteStream = false
    new LZ4BlockInputStream(
      s,
      lz4Factory.fastDecompressor(),
      xxHashFactory.newStreamingHash32(defaultSeed).asChecksum,
      disableConcatenationOfByteStream)
  }
}
