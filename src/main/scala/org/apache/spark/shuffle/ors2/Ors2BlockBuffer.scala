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

import com.esotericsoftware.kryo.io.Output
import org.apache.spark.serializer.SerializerInstance

case class Ors2BlockBuffer(ser: SerializerInstance, compression: Ors2Compression, blockSize: Int, maxBufferSize: Int) {
  private val output = new Output(blockSize, maxBufferSize)
  private var bs = compression.compressedOutputStream(output)
  private var objOutput = ser.serializeStream(bs)

  def close(): Unit = {
    objOutput.close()
    // output.release()
  }

  def write(elm: Product2[Any, Any]): Unit = {
    objOutput.writeKey(elm._1)
    objOutput.writeValue(elm._2)
  }

  def write(key: Any, value: Any): Unit = {
    objOutput.writeKey(key)
    objOutput.writeValue(value)
  }

  def flush(): Unit = {
    objOutput.flush()
  }

  def toBytes: Array[Byte] = {
    flush()

    val bytes = output.toBytes

    output.clear()
    //ByteBufferOutput can be shared. Compressed and serialized streams cannot be shared.
    bs = compression.compressedOutputStream(output)
    objOutput = ser.serializeStream(bs)

    bytes
  }

  def position: Int = {
    output.position()
  }

  def write(kvBytes: Array[Byte], offs: Int, len: Int): Unit = {
    bs.write(kvBytes, offs, len)
  }
}
