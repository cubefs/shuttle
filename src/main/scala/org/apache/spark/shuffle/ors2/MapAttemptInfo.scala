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

import com.carrotsearch.hppc.IntLongHashMap
import scala.collection.mutable

case class MapAttemptInfo(numMaps: Int, mapAttemptId: mutable.Map[Int, Long]) {
  override def toString: String = {
    val mapAttemptIdsStr = mapAttemptId.toString();
    s"MapAttemptInfo(numMaps: $numMaps, taskAttemptIds: $mapAttemptIdsStr)"
  }

  def asJavaMapAttempt(): IntLongHashMap = {
    val javaMap = new IntLongHashMap(mapAttemptId.size)

    mapAttemptId.foreach(x => {
      javaMap.put(x._1, x._2)
    })

    javaMap
  }
}
