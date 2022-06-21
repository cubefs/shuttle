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

import com.google.common.annotations.VisibleForTesting
import com.oppo.shuttle.rss.common.{Ors2WorkerDetail, StageShuffleId}
import com.oppo.shuttle.rss.exceptions.Ors2Exception
import com.oppo.shuttle.rss.metadata.ServiceManager
import com.oppo.shuttle.rss.storage.{ShuffleFileStorage, ShuffleFileUtils}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.shuffle.{Ors2Config, Ors2ShuffleServerHandle}

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * This class extends spark listener to process spark events:
 *     onApplicationEnd, onStageCompleted, onStageSubmitted etc.
 *
 * @param appId
 * @param appAttemptId
 * @param ors2Servers
 * @param networkTimeoutMillis
 */
class Ors2SparkListener(
  val appId: String,
  val appAttemptId: String,
  val networkTimeoutMillis: Int,
  val clusterConf: Ors2ClusterConf,
  val serviceManager: ServiceManager)
  extends SparkListener with Logging {
  private val shuffleServerMap = new ConcurrentHashMap[Integer, Array[Ors2WorkerDetail]]()
  private val stageToShuffleMap = new ConcurrentHashMap[Integer, Integer]()

  val storage = new ShuffleFileStorage(clusterConf.rootDir, clusterConf.fsConfBean())
  val rootDir: String = storage.getRootDir

  val deleteShuffleDir: Boolean = SchedulerUtils.conf.get(Ors2Config.deleteShuffleDir)

  def addShuffleServer(shuffleId: Int, serverHandle: Array[Ors2ShuffleServerHandle]): Unit = {
    val servers: Array[Ors2WorkerDetail] = {
      val mutableSet: mutable.Set[Ors2WorkerDetail] = mutable.Set()
      serverHandle.foreach(_.ors2ServerGroup.getServers.asScala.foreach(mutableSet.add))
      mutableSet.toArray
    }
    shuffleServerMap.put(shuffleId, servers)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    if (stageCompleted.stageInfo == null) {
      return
    }

    val shuffleId = stageToShuffleMap.get(stageCompleted.stageInfo.stageId)
    val stageAttempt = stageCompleted.stageInfo.attemptNumber()
    if (shuffleId == null || shuffleId == -1 || !shuffleServerMap.containsKey(shuffleId)) {
      logInfo(s"stage ${stageCompleted.stageInfo.stageId} no shuffle")
      return ;
    }

    val filePath = ShuffleFileUtils.getStageCompleteSignPath(
      rootDir,
      new StageShuffleId(appId, appAttemptId, stageAttempt, shuffleId))
    val parentPath = filePath.getParent.toString

    // Create a new file on cfs to tell shuffle server this stage is completed.

    try {
      storage.createWriterStream(filePath.toString, "").close()
      logInfo(s"Shuffle id $shuffleId, stage id ${stageCompleted.stageInfo.stageId}, sign path $filePath is created")
    } catch {
      case e: Throwable =>
        throw new Ors2Exception("Failed to create directories: " + parentPath, e)
    }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    if (stageSubmitted.stageInfo == null) {
      return
    }

    // Remove this shuffleId file in case the stage is rerun.
    val shuffleId = SchedulerUtils.getShuffleId(stageSubmitted.stageInfo.stageId)
    stageToShuffleMap.put(stageSubmitted.stageInfo.stageId, shuffleId)
    val stageAttempt = stageSubmitted.stageInfo.attemptNumber()

    val filePath = ShuffleFileUtils
      .getStageCompleteSignPath(
        rootDir,
        new StageShuffleId(appId, appAttemptId, stageAttempt, shuffleId))
    try {
      if (storage.exists(filePath.toString)) {
        logInfo(s"ShuffleId $shuffleId is rerun, so we delete this sign path when stage submitted")
        storage.deleteDirectory(filePath.toString)
        logInfo(s"Path $filePath is deleted successfully")
      }
    } catch {
      case e: Exception =>
        throw new Ors2Exception("Failed to check directories: " + filePath, e)
    }
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    if (shuffleServerMap.isEmpty) {
      return
    }

    val filePath = ShuffleFileUtils.getAppCompleteSignPath(rootDir, appId, appAttemptId)
    val parentPath = filePath.getParent.toString
    try {
      storage.createWriterStream(filePath.toString, "").close()
      logInfo(s"app $appId is complete, sign path $filePath is created")
    } catch {
      case e: Throwable =>
        throw new Ors2Exception("Failed to create directories: " + parentPath, e)
    }
  }
}

object Ors2SparkListener extends Logging {

  @volatile private var instance: Ors2SparkListener = _

  /**
   * Init once the Ors2SparkListener and register it to SparkContext
   * @param context
   * @param appId
   * @param attemptId
   * @param ors2Servers
   * @param timeoutMillSec
   */
  def registerListener(
    context: SparkContext,
    appId: String,
    attemptId: String,
    shuffleId: Int,
    ors2Servers: Array[Ors2ShuffleServerHandle],
    timeoutMillSec: Int,
    clusterConf: Ors2ClusterConf,
    serviceManager: ServiceManager
  ): Unit = {
    if (instance == null) {
      this.synchronized {
        if (instance == null) {
          instance = new Ors2SparkListener(appId, attemptId, timeoutMillSec, clusterConf, serviceManager)
          context.addSparkListener(instance)
          logInfo("Registered Ors2SparkListener.")
        }
      }
    }
    instance.addShuffleServer(shuffleId, ors2Servers)
  }

  @VisibleForTesting
  def clearListenerInstance(): Unit = this.synchronized {
    instance = null
  }
}
