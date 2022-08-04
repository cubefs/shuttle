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

import com.oppo.shuttle.rss.BuildVersion
import com.oppo.shuttle.rss.clients.Ors2ClientFactory
import com.oppo.shuttle.rss.common.{AppTaskInfo, Constants, Ors2ServerGroup, Ors2WorkerDetail, StageShuffleId}
import com.oppo.shuttle.rss.exceptions.{Ors2Exception, Ors2NoShuffleWorkersException}
import com.oppo.shuttle.rss.metadata.{Ors2MasterServerManager, ServiceManager, ZkShuffleServiceManager}
import com.oppo.shuttle.rss.util.ShuffleUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SchedulerUtils
import org.apache.spark.shuffle.ors2.{Ors2BlockManager, Ors2ClusterConf, Ors2ShuffleReadMetrics, Ors2ShuffleWriteMetrics, Ors2SparkListener}
import org.apache.spark.shuffle.sort.{Ors2UnsafeShuffleWriter, SortShuffleManager, SortShuffleWriter}
import org.apache.spark.sql.internal.SQLConf.{ADAPTIVE_EXECUTION_ENABLED, LOCAL_SHUFFLE_READER_ENABLED}

import scala.collection.JavaConverters._
import scala.util.{Random => SRandom}

class Ors2ShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {
  logInfo(s"Creating ShuffleManager instance: ${this.getClass.getSimpleName}, project version: ${BuildVersion.projectVersion}," +
    s" git commit revision: ${BuildVersion.gitCommitVersion}")

  private val SparkYarnQueueConfigKey = "spark.yarn.queue"

  private val networkTimeoutMillis = conf.get(Ors2Config.networkTimeout).toInt
  private val networkRetries = conf.get(Ors2Config.getClientMaxRetries)
  private val retryInterval = conf.get(Ors2Config.ioRetryWait)
  private val inputReadyQueryInterval = conf.get(Ors2Config.dataInputReadyQueryInterval)
  private val inputReadyMaxWaitTime = conf.get(Ors2Config.shuffleReadWaitFinalizeTimeout)

  private val dataCenter = conf.get(Ors2Config.dataCenter)
  private val cluster = conf.get(Ors2Config.cluster)
  private val useEpoll = conf.get(Ors2Config.useEpoll)
  private var masterName = conf.get(Ors2Config.masterName)
  private val isGetActiveMasterFromZk: Boolean = conf.get(Ors2Config.isGetActiveMasterFromZk)
  private val dagId = conf.get(Ors2Config.dagId)
  private val jobPriority = conf.get(Ors2Config.jobPriority)
  private val taskId = conf.get(Ors2Config.taskId)
  private val appName = ShuttleOnYarn.getAppName(conf)
  val dfsDirPrefix: String = conf.get(Ors2Config.dfsDirPrefix)

  private var serviceManager: ServiceManager = _

  private val clientFactory: Ors2ClientFactory = new Ors2ClientFactory(conf)

  private def getSparkContext = {
    SparkContext.getActive.get
  }

  /**
   * Called by Spark app driver.
   * Fetch Ors2 shuffle workers to use.
   * Return a ShuffleHandle to driver for (getWriter/getReader).
   */
  override def registerShuffle[K, V, C](shuffleId: Int, dependency: ShuffleDependency[K, V, C]):
  ShuffleHandle = {
    logInfo(s"Use ShuffleManager: ${this.getClass.getSimpleName}")

    if (conf.get(ADAPTIVE_EXECUTION_ENABLED) && conf.get(LOCAL_SHUFFLE_READER_ENABLED)) {
      throw new Ors2Exception(s"shuttle rss does not support local file reading. " +
        s"Please set ${LOCAL_SHUFFLE_READER_ENABLED.key} to false")
    }

    val blockSize = conf.get(Ors2Config.writeBlockSize)
    val minBlockSize = conf.get(Ors2Config.minWriteBlockSize)
    val maxBlockSize = conf.get(Ors2Config.maxWriteBlockSize)
    if (blockSize < minBlockSize || blockSize > maxBlockSize) {
      throw new RuntimeException(s"config ${Ors2Config.writeBlockSize.key} must be between ${minBlockSize} and ${maxBlockSize}")
    }

    val numPartitions = dependency.partitioner.numPartitions
    val sparkContext = getSparkContext
    val user = sparkContext.sparkUser
    val queue = conf.get(SparkYarnQueueConfigKey, "")

    val appId = conf.getAppId
    val appAttempt = sparkContext.applicationAttemptId.getOrElse("0")

    val (clusterConf, shuffleWorkers) = getShuffleWorkers(numPartitions, appId)
    val (partitionMapToShuffleWorkers, ors2Servers) = distributeShuffleWorkersToPartition(shuffleId, numPartitions, shuffleWorkers)

    logInfo(s"partitionMapToShuffleWorkers to shuffle id $shuffleId size: ${partitionMapToShuffleWorkers.size}: $partitionMapToShuffleWorkers")

    Ors2SparkListener.registerListener(sparkContext, conf.getAppId, appAttempt, shuffleId,
      ors2Servers, networkTimeoutMillis, clusterConf, getOrCreateServiceManager)

    val dependencyInfo = s"numPartitions: ${dependency.partitioner.numPartitions}, " +
      s"serializer: ${dependency.serializer.getClass.getSimpleName}, " +
      s"keyOrdering: ${dependency.keyOrdering}, " +
      s"aggregator: ${dependency.aggregator}, " +
      s"mapSideCombine: ${dependency.mapSideCombine}, " +
      s"keyClassName: ${dependency.keyClassName}, " +
      s"valueClassName: ${dependency.valueClassName}"

    logInfo(s"RegisterShuffle: $appId, $appAttempt, $shuffleId, $dependencyInfo")

    new Ors2ShuffleHandle(
      shuffleId,
      dependency.rdd.getNumPartitions,
      appId,
      appAttempt,
      user,
      queue,
      dependency,
      partitionMapToShuffleWorkers,
      ors2Servers,
      clusterConf
    )
  }

  /**
   * Called by Spark app executors, get ShuffleWriter from Spark driver via the ShuffleHandle.
   */
  override def getWriter[K, V](
    handle: ShuffleHandle,
    mapId: Long,
    context: TaskContext,
    metrics: ShuffleWriteMetricsReporter
  ): ShuffleWriter[K, V] = {
    handle match {
      case ors2ShuffleHandle: Ors2ShuffleHandle[K@unchecked, V@unchecked, _] => {
        val mapInfo = new AppTaskInfo(
          conf.getAppId,
          ors2ShuffleHandle.appAttempt,
          ors2ShuffleHandle.partitionMapToShuffleWorkers.size,
          conf.get(Ors2Config.partitionCountPerShuffleWorker),
          ors2ShuffleHandle.shuffleId,
          context.partitionId(),
          context.attemptNumber(),
          context.stageAttemptNumber()
        )

        val blockManager = Ors2BlockManager(
          taskContext = context,
          numPartitions = ors2ShuffleHandle.dependency.partitioner.numPartitions,
          partitionMapToShuffleWorkers = ors2ShuffleHandle.partitionMapToShuffleWorkers,
          appTaskInfo = mapInfo,
          ors2Servers = ors2ShuffleHandle.getServerList,
          Ors2ShuffleWriteMetrics(metrics),
          conf.get(Ors2Config.writerBufferSpill).toInt,
          clientFactory
        )

        var writeType = conf.get(Ors2Config.shuffleWriterType)
        if (writeType == Ors2Config.SHUFFLE_WRITER_AUTO) {
          if (SortShuffleWriter.shouldBypassMergeSort(conf, ors2ShuffleHandle.dependency)) {
            writeType = Ors2Config.SHUFFLE_WRITER_BYPASS
          } else if (SortShuffleManager.canUseSerializedShuffle(ors2ShuffleHandle.dependency)) {
            writeType = Ors2Config.SHUFFLE_WRITER_UNSAFE
          } else {
            writeType = Ors2Config.SHUFFLE_WRITER_SORT
          }
        }

        val writer: ShuffleWriter[K, V] = writeType match {
          case Ors2Config.SHUFFLE_WRITER_BYPASS =>
            Ors2BypassShuffleWriter(
              blockManager,
              context.taskMemoryManager(),
              ors2ShuffleHandle.dependency,
              context,
              conf
            )
          case Ors2Config.SHUFFLE_WRITER_UNSAFE =>
            new Ors2UnsafeShuffleWriter(
              blockManager,
              context.taskMemoryManager(),
              ors2ShuffleHandle.dependency,
              context,
              conf
            )
          case Ors2Config.SHUFFLE_WRITER_SORT =>
            Ors2SortShuffleWriter(
              blockManager,
              context.taskMemoryManager(),
              ors2ShuffleHandle.dependency,
              context,
              conf
            )
          case _ => throw new RuntimeException(s"not support: $writeType")
        }

        val numPartitions = ors2ShuffleHandle.dependency.partitioner.numPartitions
        logInfo(s"shuttle rss writer use ${writer.getClass.getSimpleName}. " +
          s"$handle, numPartitions: $numPartitions, mapId: $mapId, stageId: ${context.stageId()}, shuffleId: ${handle.shuffleId}")
        writer
      }
      case _ => throw new RuntimeException(s"not support: $handle")
    }
  }

  /**
   * Called by Spark executors, get ShuffleReader from Spark driver via the ShuffleHandle.
   */
  override def getReader[K, C](
    handle: ShuffleHandle,
    startMapIndex: Int,
    endMapIndex: Int,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    getReaderForRange(handle, startMapIndex, endMapIndex, startPartition, endPartition, context, metrics)
  }

  def getReaderForRange[K, C](
    handle: ShuffleHandle,
    startMapIndex: Int,
    endMapIndex: Int,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    metrics: ShuffleReadMetricsReporter
  ): ShuffleReader[K, C] = {
    logInfo(s"shuttle rss getReaderForRange: Use ShuffleManager: " +
      s"${this.getClass.getSimpleName}, $handle, partitions: [$startPartition, $endPartition)")

    val Ors2ShuffleHandle = handle.asInstanceOf[Ors2ShuffleHandle[K, _, C]]
    val stageShuffleId = new StageShuffleId(
      conf.getAppId,
      Ors2ShuffleHandle.appAttempt,
      context.stageAttemptNumber(),
      handle.shuffleId
    )

    val serializer = Ors2ShuffleHandle.dependency.serializer

    new Ors2ShuffleReader(
      user = Ors2ShuffleHandle.user,
      clusterConf = Ors2ShuffleHandle.clusterConf,
      stageShuffleId = stageShuffleId,
      startMapIndex = startMapIndex,
      endMapIndex = endMapIndex,
      startPartition = startPartition,
      endPartition = endPartition,
      serializer = serializer,
      context = context,
      conf = conf,
      shuffleDependency = Ors2ShuffleHandle.dependency,
      inputReadyCheckInterval = inputReadyQueryInterval,
      inputReadyWaitTime = inputReadyMaxWaitTime,
      shuffleMetrics = Ors2ShuffleReadMetrics(metrics)
    )
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    true
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = {
    new Ors2ShuffleBlockResolver()
  }

  override def stop(): Unit = {
    try {
      if (serviceManager != null) {
        serviceManager.close()
      }
      clientFactory.stop()
    } catch {
      case e: Throwable => log.error("Stop error", e)
    }
  }

   def getOrCreateServiceManager: ServiceManager = {
    if (serviceManager != null) {
      return serviceManager
    }

    val zkManager = new ZkShuffleServiceManager(getZooKeeperServers, networkTimeoutMillis, networkRetries)

    val serviceManagerType = conf.get(Ors2Config.serviceManagerType)
    serviceManager = serviceManagerType match {
      case Constants.MANAGER_TYPE_ZK =>
        zkManager
      case Constants.MANAGER_TYPE_MASTER =>
        initActiveMaster(zkManager)
        new Ors2MasterServerManager(zkManager, networkTimeoutMillis, retryInterval, masterName, useEpoll)
      case _ => throw new RuntimeException(s"Invalid service registry type: $serviceManagerType")
    }

    serviceManager
  }

  private def getZooKeeperServers: String = {
    var serversValue = conf.get(Ors2Config.serviceRegistryZKServers)
    // Translation compatible with old configuration
    if (StringUtils.isEmpty(serversValue)) {
      serversValue = conf.get("spark.shuffle.ors2.serviceRegistry.zookeeper.servers")
    }
    serversValue
  }

  private def initActiveMaster(zkManager: ZkShuffleServiceManager): Unit = {
    if (isGetActiveMasterFromZk) {
      // Try to get the currently used master from zk
      val zkActiveMaster = zkManager.getActiveCluster
      if (zkActiveMaster != null) {
        masterName = zkActiveMaster
      } else {
        log.warn("Active master name is not set in zk path /shuffle_rss_path/use_cluster/shuffle_master, " +
          "so use the default master name")
      }
    }
    logInfo(s"shuttle rss use master $masterName, isGetActiveMasterFromZk $isGetActiveMasterFromZk")
  }

  def randomItem[T](items: Array[T]): T = {
    items(SRandom.nextInt(items.length))
  }

  protected[spark] def distributeShuffleWorkersToPartition(
    shuffleId: Int,
    numPartitions: Int,
    workers: Array[Ors2WorkerDetail]): (Map[Int, Int], Array[Ors2ShuffleServerHandle]) = {
    val workersPerGroup = SchedulerUtils.getWorkerGroupSize(shuffleId)

    val serverDetails = SRandom.shuffle(workers.toList)

    val serverGroup = serverDetails.indices.map(id => {
      id.until(workersPerGroup + id).map(idx => {
        val i = idx % serverDetails.length
        serverDetails(i)
      }).distinct.toList
    }).toArray

    if (serverGroup.isEmpty) {
      throw new Ors2NoShuffleWorkersException("There is no reachable shuttle rss server")
    }
    logInfo(s"shuttle rss server assign to shuffle id $shuffleId group size: ${serverGroup.length}," +
      s" workersPerGroup: ${workersPerGroup}, serverCombinations:")

    serverGroup.indices.foreach(id => {
      logInfo(s"ShuffleWorkerGroupIdx: $id, size: ${serverGroup(id).length},  workers: ${serverGroup(id)}")
    })

    val shuffleHandles = serverGroup
      .map(group => Ors2ShuffleServerHandle(new Ors2ServerGroup(group.asJava)))

    // Workers used by the partition, evenly distributed
    val partitionMapToShuffleWorkers = 0.until(numPartitions).map(part => {
      (part, part % serverGroup.length)
    }).toMap

    (partitionMapToShuffleWorkers, shuffleHandles)
  }

  /**
   * Fetch Ors2 shuffleWorkers, from zk or shuffleMaster
   *
   * @param numPartitions
   * @return
   */
  def getShuffleWorkers(numPartitions: Int, appId: String): (Ors2ClusterConf, Array[Ors2WorkerDetail]) = {
    logInfo(s"getShuffleWorkers numPartitions: $numPartitions")
    val maxServerCount = conf.get(Ors2Config.maxRequestShuffleWorkerCount)
    val minServerCount = conf.get(Ors2Config.minRequestShuffleWorkerCount)
    val partitionCountPerShuffleWorker = conf.get(Ors2Config.partitionCountPerShuffleWorker)

    var requestWorkerCount = Math.ceil(numPartitions / partitionCountPerShuffleWorker.toDouble).toInt

    if (requestWorkerCount < minServerCount) {
      requestWorkerCount = minServerCount
    }

    if (requestWorkerCount > maxServerCount) {
      requestWorkerCount = maxServerCount
    }

    val configuredServerList = ShuffleUtils.getShuffleServersWithoutCheck(
      getOrCreateServiceManager,
      requestWorkerCount,
      networkTimeoutMillis,
      dataCenter,
      cluster,
      appId,
      dagId,
      numPartitions,
      taskId,
      appName)

    if (configuredServerList.getServerDetailList.isEmpty) {
      throw new Ors2NoShuffleWorkersException("There is no reachable ors2 server")
    }

    val clusterConf = if (StringUtils.isEmpty(configuredServerList.getConf)) {
      Ors2ClusterConf(dfsDirPrefix, dataCenter, cluster, "")
    } else {
      Ors2ClusterConf(configuredServerList.getRootDir, configuredServerList.getDataCenter,
        configuredServerList.getCluster, configuredServerList.getConf)
    }
    log.info(s"use shuffle cluster: $clusterConf")

    (clusterConf, configuredServerList.getServerDetailArray)
  }
}
