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

import com.oppo.shuttle.rss.clients.{NettyClient, Ors2ClientFactory}
import com.oppo.shuttle.rss.common.{AppTaskInfo, Ors2ServerGroup, Ors2ServerSwitchGroup, Ors2WorkerDetail}
import org.apache.spark.SparkConf
import org.testng.annotations.Test

import java.util
import java.util.Optional
import java.util.concurrent.CountDownLatch

class Ors2ServerSwitchTest {

  val details = new util.ArrayList[Ors2WorkerDetail]
  details.add(new Ors2WorkerDetail("host-1", 19190, 19191))
  details.add(new Ors2WorkerDetail("host-2", 19190, 19191))
  details.add(new Ors2WorkerDetail("host-3", 19190, 19191))
  val serverGroup = new Ors2ServerSwitchGroup(util.Arrays.asList(new Ors2ServerGroup(details)), 0, 3, false)

  @Test
  def normal(): Unit = {
    val server = serverGroup.getServer(0, 0, Optional.empty())
    println(server)
    assert(server.equals(details.get(0)))
  }

  @Test
  def retry(): Unit = {
    var server = serverGroup.getServer(0, 1, Optional.empty())
    println(server)
    assert(server.equals(details.get(0)))

    server = serverGroup.getServer(0, 3, Optional.empty())
    println(server)
    assert(server.equals(details.get(1)))

    server = serverGroup.getServer(0, 6, Optional.empty())
    println(server)
    assert(server.equals(details.get(2)))
  }

  @Test
  def serverError(): Unit = {
    var server = serverGroup.getServer(0, 100, Optional.of(details.get(0)))
    println(server)
    assert(server.equals(details.get(1)))

    server = serverGroup.getServer(0, 100, Optional.of(details.get(0)))
    println(server)
    assert(server.equals(details.get(1)))
  }

  @Test
  def threadSafe(): Unit = {
    val latch = new CountDownLatch(100)
    1.to(100).foreach(_ => {
      new Thread() {
        override def run(): Unit = {
          1.to(1000).foreach(_ => {
            val server = serverGroup.getServer(0, 100, Optional.of(details.get(0)))
            assert(server.equals(details.get(1)))
          })
          latch.countDown()
        }
      }.start()
    })

    latch.await()
  }

  // @Test
  def connect(): Unit = {
    val conf = new SparkConf()
    conf.set("spark.shuffle.rss.io.maxRetries", "1")
    val env = Ors2ShuffleTestEnv.masterService()
    env.startCluster()

    // close first worker
    env.restartFirstServer()

    val appTaskInfo = new AppTaskInfo("1", "0", 1,
      20, 0, 1, 0, 0)
    val factory = new Ors2ClientFactory(conf)
    val client = new NettyClient(env.getCluster.serverGroup(), conf, appTaskInfo, factory)

    // connect second worker
    val shuffleClient = client.getClient(0, 0, Optional.empty())
    assert(shuffleClient._1.getServer.equals(env.getCluster.serverList.get(1)))

    env.stopCluster()
  }
}
