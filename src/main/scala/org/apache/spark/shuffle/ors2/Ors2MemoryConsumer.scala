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

import com.oppo.shuttle.rss.exceptions.Ors2Exception
import org.apache.spark.internal.Logging
import org.apache.spark.memory.{MemoryConsumer, TaskMemoryManager}
import org.apache.spark.util.Utils

import scala.collection.mutable

class Ors2MemoryConsumer(taskMemoryManager: TaskMemoryManager) extends MemoryConsumer(taskMemoryManager) with Logging{
  private val SAMPLE_GROWTH_RATE = 1.01

  private val map = mutable.Map[String, Int]()

  override def spill(size: Long, trigger: MemoryConsumer): Long = {
    0
  }

  def acquireMemory(key: String, size: Long): Long = {
    if (map.contains(key)) {
      throw new Ors2Exception(s"$key already allocated memory")
    }

    val acquireSize = Math.ceil(size * SAMPLE_GROWTH_RATE).toInt
    val granted = super.acquireMemory(acquireSize)
    if (granted != acquireSize) {
      throw new Ors2Exception(s"Failed to allocate memory ${Utils.bytesToString(size)}:" +
        s" not enough memory for $key, try to reduce the configuration memory or increase the executor memory")
    }
    log.info(s"acquire heap memory ${Utils.bytesToString(acquireSize)} for $key")
    map.put(key, acquireSize)
    granted
  }

  def freeMemory(key: String): Unit = {
    map.get(key) match {
      case None => log.warn(s"Not found $key")
      case Some(size) =>
        freeMemory(size)
        log.info(s"free heap memory ${Utils.bytesToString(size)} for $key")
    }
  }
}
