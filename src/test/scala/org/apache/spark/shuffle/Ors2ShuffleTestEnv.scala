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

import com.oppo.shuttle.rss.common.Constants
import com.oppo.shuttle.rss.server.master.WeightedRandomDispatcher
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.test.TestingServer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.testutil.{Ors2MiniCluster, Ors2ShuffleMaster}

class Ors2ShuffleTestEnv(numServer: Int, zkConn: Option[String], serviceRegistryType: String) {
  @volatile private var isStart = false

  private var zkServer: TestingServer = _
  private var zkClient: CuratorFramework = _
  private var realZkConn: String = _
  private var cluster: Ors2MiniCluster = _
  private var shuffleMaster: Ors2ShuffleMaster = _

  def startCluster(): Unit = {
    realZkConn = if (zkConn.isEmpty) {
      // start local zk
      startZk()
      "localhost:2181"
    } else {
      zkConn.get
    }

    cluster = new Ors2MiniCluster(numServer, realZkConn, serviceRegistryType,
      Constants.TEST_DATACENTER_DEFAULT, Constants.TEST_CLUSTER_DEFAULT)

    startServer()

    isStart = true
  }

  private def startZk(): Unit = {
    zkServer = new TestingServer(2181, true)
    zkServer.start()

    zkClient = CuratorFrameworkFactory.newClient("127.0.0.1",
      new ExponentialBackoffRetry(1000, 3))

    zkClient.start()
  }

  private def startServer(): Unit = {
    this.synchronized {
      if (!isStart) {
        if (serviceRegistryType.equals(Constants.MANAGER_TYPE_MASTER)){
          shuffleMaster = new Ors2ShuffleMaster(realZkConn, classOf[WeightedRandomDispatcher].getName, 19189, 19188)
          shuffleMaster.start()
        }

        isStart = true
        cluster.start()
      }
    }
  }

  def stopCluster(): Unit = {
    cluster.shutdown()
    if (shuffleMaster != null) shuffleMaster.stop()
    if (zkServer != null) zkServer.stop()
    if (zkClient != null) zkClient.close()
  }

  def restartFirstServer(): Unit = {
    val worker = cluster.shuffleWorkers.head
    worker.shutdown()
    worker.run()
  }


  def sparkConf(): SparkConf = {
    new SparkConf()
      .setMaster("local[*]")
      .setAppName("rss-test")
      .set("spark.executor.memory", "1g")
      .set("spark.executor.cores", "1")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.Ors2ShuffleManager")
      .set(Ors2Config.serviceManagerType, serviceRegistryType)
      .set(Ors2Config.serviceRegistryZKServers, realZkConn)
      .set(Ors2Config.dataCenter, cluster.dataCenter)
      .set(Ors2Config.cluster, cluster.cluster)
      .set(Ors2Config.dfsDirPrefix, cluster.shuffleDir)
      .set(Ors2Config.shuffleWriterType, Ors2Config.SHUFFLE_WRITER_AUTO)
      .set(Ors2Config.shuffleReadMaxSize.key, "8mb")
      .set(Ors2Config.shuffleReadMergeSize.key, "1mb")
      .set(Ors2Config.deleteShuffleDir.key, "true")
      .set(Ors2Config.writerBufferSpill.key, "8mb")
      .set(Ors2Config.ioMaxRetry, 1)
      .set(Ors2Config.ioRetryWait, 1L)
      .set(Ors2Config.partitionCountPerShuffleWorker, 2)
      .set("spark.shuffle.sort.bypassMergeThreshold", "1")
  }


  def createSession(): SparkSession = {
    createSession(sparkConf())
  }

  def createSession(conf: SparkConf): SparkSession = this.synchronized {
    val mergeConf = sparkConf()
    mergeConf.setAll(conf.getAll)

    SparkSession.builder()
      .config(mergeConf)
      .getOrCreate()
  }

  def getCluster: Ors2MiniCluster = {
    cluster
  }
}

object Ors2ShuffleTestEnv {
  lazy val env: Ors2ShuffleTestEnv = {
    val env = masterService()
    env.startCluster()
    env
  }


  def masterService(): Ors2ShuffleTestEnv = {
    new Ors2ShuffleTestEnv(3, None, Constants.MANAGER_TYPE_MASTER)
  }

  def zkService(): Ors2ShuffleTestEnv = {
    new Ors2ShuffleTestEnv(3, None, Constants.MANAGER_TYPE_ZK)
  }
}
