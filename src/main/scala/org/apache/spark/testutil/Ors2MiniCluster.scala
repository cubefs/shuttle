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

import com.oppo.shuttle.rss.ShuffleServerConfig
import com.oppo.shuttle.rss.common.{Constants, Ors2ServerGroup, Ors2WorkerDetail}
import com.oppo.shuttle.rss.server.worker.ShuffleWorker
import com.oppo.shuttle.rss.storage.ShuffleFileStorage
import com.oppo.shuttle.rss.util.NetworkUtils
import org.slf4j.LoggerFactory

import java.io.File
import java.util
import scala.collection.mutable.ListBuffer


/**
 * ors2 test cluster
 */
class Ors2MiniCluster(val numServer: Int,
  val zkServer: String,
  val serviceRegistryType: String,
  val dataCenter: String,
  val cluster: String) {
  private val logger = LoggerFactory.getLogger(classOf[Ors2MiniCluster])

  val shuffleWorkers: ListBuffer[ShuffleWorker] = ListBuffer()

  val shuffleDir = s"rss-data"

  val stateDir = s"state-data"

  val startPort = 19190

  val serverList: java.util.ArrayList[Ors2WorkerDetail] = new java.util.ArrayList()

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      new File(shuffleDir).delete()
      new File(stateDir).delete()
    }
  })

  def this(zkServer: String) {
    this(3, zkServer, "zookeeper", Constants.TEST_DATACENTER_DEFAULT, Constants.TEST_CLUSTER_DEFAULT)
  }

  def start(): Unit = {
    0.until(numServer).foreach(i => {
      val worker = createServer(startPort + i * 2, startPort + i * 2 + 1, i)
      serverList.add(new Ors2WorkerDetail(NetworkUtils.getLocalIp, startPort + i * 2, startPort + i * 2 + 1))
      shuffleWorkers.append(worker)
      worker.run()
      if (serviceRegistryType == Constants.MANAGER_TYPE_MASTER) worker.initWorkerClient()
    })
  }

  def createConfig(dataPort: Int, buildPort: Int, index: Int): ShuffleServerConfig = {
    val config = new ShuffleServerConfig
    config.setUseEpoll(false)
    config.setRootDirectory(shuffleDir)
    config.setZooKeeperServers(zkServer)
    config.setDataCenter(dataCenter)
    config.setCluster(cluster)
    config.setStorage(new ShuffleFileStorage(shuffleDir))
    config.setBaseConnections(100)
    config.setMaxConnections(100)
    config.setMemoryControlSizeThreshold(1024 * 1024 * 512)
    config.setMemoryControlRatioThreshold(0.95F)
    config.setWorkerLoadWeight(index + 1)
    config.setBuildConnectionPort(buildPort)
    config.setShufflePort(dataPort)

    config
  }

  def createServer(dataPort: Int, buildPort: Int, index: Int): ShuffleWorker = {
    val config = createConfig(dataPort, buildPort, index)
    new ShuffleWorker(config)
  }

  def shutdown(): Unit = {
    shuffleWorkers.foreach(_.shutdown())
  }

  def serverGroup(): util.List[Ors2ServerGroup] = {
    util.Arrays.asList(new Ors2ServerGroup(serverList))
  }
}
