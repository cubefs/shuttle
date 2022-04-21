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

import com.oppo.shuttle.rss.common.{AppTaskInfo, Ors2ServerGroup, ShuffleInfo}
import com.oppo.shuttle.rss.util.CommonUtils
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.BlockManagerId

import java.util.Base64
import java.util.function.Supplier

object Ors2Util extends Logging {

  /**
   * Get ShuffleInfo.MapShuffleInfo protobuf msg obj from BlockManagerId
   * @param blockManagerId
   */
  def getMapShuffleInfo(blockManagerId: BlockManagerId): Option[ShuffleInfo.MapShuffleInfo] = {
    val tpInfoStr = blockManagerId.topologyInfo.getOrElse("")
    if (tpInfoStr.isEmpty) {
      return None
    }
    val bytes = Base64.getDecoder.decode(tpInfoStr)
    val mapShuffleInfo = ShuffleInfo.MapShuffleInfo.parseFrom(bytes)
    Some(mapShuffleInfo)
  }

  /** *
   * Get map task attempt from map output tracker.
   * This method will get all the map tasks attempts. Return as Map[taskId, attemptId]
   *
   * @param shuffleId shuffle id
   * @param partition partition id
   * @return MapAttemptInfo
   */
  def getMapTaskAttempt(shuffleId: Int,  startMapIndex: Int, endMapIndex: Int,
    partition: Int, retryIntervalMillis: Long, maxRetryMillis: Long): Option[MapAttemptInfo] = {
    // mapLatestAttempt, key: mapId, value: latest attemptId
    val mapAttemptInfoList =
      CommonUtils.retry(retryIntervalMillis,
        maxRetryMillis,
        new Supplier[Seq[ShuffleInfo.MapShuffleInfo]] {
          override def get(): Seq[ShuffleInfo.MapShuffleInfo] = {
            val mapStatusInfo = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(shuffleId, startMapIndex, endMapIndex, partition, partition + 1)
            logInfo(s"Got result from mapOutputTracker.getMapSizesByExecutorId")
            mapStatusInfo
              .toParArray
              .flatMap(mapStatusInfoEntry => getMapShuffleInfo(mapStatusInfoEntry._1))
              .toList
          }
        })
    logInfo(s"Got ${mapAttemptInfoList.size} items after parsing mapOutputTracker.getMapSizesByExecutorId result")
    if (mapAttemptInfoList.isEmpty) {
      return None
    }

    val mapLatestAttemptIds = mapAttemptInfoList.groupBy(f => f.getMapId).map {
      case(k, v) => {
        k -> {
          v.reduce((x, y) => { if (x.getAttemptId < y.getAttemptId){y} else x } )
        }
      }
    }.map{ case (k, v) => {k -> v.getAttemptId} }

    val mutableMapWrapper = collection.mutable.Map(mapLatestAttemptIds.toSeq: _*)
    val numMaps = mutableMapWrapper.size

    Some(MapAttemptInfo(numMaps, mutableMapWrapper))
  }

  /**
   * Mock a MapShuffleInfo pb, serialize it to string,
   * mock BlockManagerId with the string to adapt spark native logic
   * @param mapId map task id
   * @param attemptId map task attempt id
   * @param ors2Servers ors2 shuffle worker groups list
   */
  def mockMapBlockManagerId(
    mapId: Int,
    attemptId: Long,
    ors2Servers: List[Ors2ServerGroup]): BlockManagerId = {
    val mockHost = "mock_host"
    val mockPort = 11111
    val mapShuffleInfoBuilder = ShuffleInfo.MapShuffleInfo.newBuilder();
    mapShuffleInfoBuilder.setMapId(mapId);
    mapShuffleInfoBuilder.setAttemptId(attemptId)
    mapShuffleInfoBuilder.setShuffleWorkerGroupNum(ors2Servers.size)
    val tpInfoStr = Base64.getEncoder.encodeToString(mapShuffleInfoBuilder.build().toByteArray)
    BlockManagerId(s"map_$mapId" + s"_$attemptId", mockHost, mockPort, Some(tpInfoStr))
  }

  /**
   * Create mock BlockManagerId for reduce task.
   * @param shuffleId shuffle id
   * @param partition partition id
   * @return
   */
  def mockReduceBlockManagerId(shuffleId: Int, partition: Int): BlockManagerId = {
    val mockHost = "mock_host"
    val mockPort = 10000
    BlockManagerId(s"reduce_${shuffleId}_$partition", mockHost, mockPort, None)
  }

  def throwableFetchFailedException(ex: Throwable, shuffleId: Int, partition: Int): Unit = {
    throw new FetchFailedException(
      mockReduceBlockManagerId(shuffleId, partition),
      shuffleId,
      -1,
      -1,
      partition,
      s"Failed to read data for shuffle $shuffleId partition $partition due to ${ex.getMessage})",
      ex)
  }

  def createMapStatus(blockManagerId: BlockManagerId, partitionLengths: Array[Long], appTaskInfo: AppTaskInfo): MapStatus = {
    MapStatus(blockManagerId, partitionLengths, appTaskInfo.getTaskAttemptId)
  }
}
