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

package org.apache.spark.shuffle

import com.oppo.shuttle.rss.common.Ors2ServerGroup
import org.apache.spark.ShuffleDependency
import org.apache.spark.shuffle.ors2.Ors2ClusterConf

private[spark] class Ors2ShuffleHandle[K, V, C](
    shuffleId: Int,
    val numMaps: Int,
    val appId: String,
    val appAttempt: String,
    val user: String,
    val queue: String,
    val dependency: ShuffleDependency[K, V, C],
    val partitionMapToShuffleWorkers: Map[Int, Int],
    val ors2Servers: Array[Ors2ShuffleServerHandle],
    val clusterConf: Ors2ClusterConf
)
  extends ShuffleHandle(shuffleId) {

  def getServerList: List[Ors2ServerGroup] = {
    ors2Servers.map(_.ors2ServerGroup).toList
  }

  override def toString: String = s"Ors2ShuffleHandle " +
    s"(shuffleId $shuffleId, cluster: ${clusterConf.dataCenter}/${clusterConf.cluster}," +
    s"rootDir: ${clusterConf.rootDir}, ors2Servers: ${ors2Servers.length} servers)"
}

