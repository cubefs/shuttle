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

import io.netty.channel.DefaultEventLoop
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.shuffle.ors2.Ors2SparkListener
import org.apache.spark.sql.functions.{count, sum}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.testng.annotations.{AfterClass, AfterMethod, BeforeClass, Test}

import java.util.concurrent.TimeUnit

/**
 * This is the most important test, a complete flow of running a spark app using ors2. Contains the following steps:
 * 1. Start zk, if necessary
 * 2. Start the shuffle master, if necessary
 * 3. Start multiple shuffle workers
 * 4. Run the spark app
 *
 * @author oppo
 */
class Ors2ShuffleManagerTest {
  private var env: Ors2ShuffleTestEnv = _

  @BeforeClass
  def startEnv(): Unit = {
    env = Ors2ShuffleTestEnv.masterService()
    env.startCluster()
  }

  @AfterClass
  def afterEnv(): Unit = {
    env.stopCluster()
  }

  @AfterMethod
  def afterTestMethod(): Unit = {
    Ors2SparkListener.clearListenerInstance()

    SparkSession.getActiveSession.foreach(_.stop())
  }

  def checkMethod(rdd: Array[Row]): Unit = {
    rdd.foreach(x => {
      val count = x.getAs[Long]("count")
      val sum = x.getAs[Long]("sum")

      assert(count == 10000)
      assert(sum == 50005000)
      println(s"count = $count, sum = $sum")
    })
  }

  def runWithConf(conf: SparkConf): Unit = {
    val spark: SparkSession = env.createSession(conf)
    import spark.implicits._
    val df = 1.to(10000).toDF("value")
      .repartition(10)
      .agg(
        count("value") as "count",
        sum("value") as "sum"
      )

    checkMethod(df.take(1))
  }

  /**
   * use Ors2BypassShuffleWriter
   */
  @Test
  def bypass(): Unit = {
    val conf = new SparkConf()
      .set(Ors2Config.shuffleWriterType, Ors2Config.SHUFFLE_WRITER_BYPASS)
    runWithConf(conf)
  }

  /**
   * use Ors2UnsafeShuffleWriter
   */
  @Test
  def unsafe(): Unit = {
    val conf = new SparkConf()
      .set(Ors2Config.shuffleWriterType, Ors2Config.SHUFFLE_WRITER_UNSAFE)
    runWithConf(conf)
  }

  /**
   * use Ors2SortShuffleWriter
   */
  @Test
  def sort(): Unit = {
    val conf = new SparkConf()
      .set(Ors2Config.shuffleWriterType, Ors2Config.SHUFFLE_WRITER_SORT)
    runWithConf(conf)
  }

  @Test
  def retry(): Unit = {
    val executor = new DefaultEventLoop()

    executor.schedule(new Runnable {
      override def run(): Unit = {
        env.restartFirstServer()
      }
    }, 2, TimeUnit.SECONDS)

    runWithConf(new SparkConf())
    executor.shutdownGracefully()
  }

  @Test
  def rddCacheMemory(): Unit = {
    val spark: SparkSession = env.createSession()

    val df = spark.sparkContext.makeRDD(1.to(10000))
      .persist(StorageLevel.MEMORY_ONLY)

    val count = df.count()
    assert(count == 10000)

    assert(df.reduce(_ + _) == 50005000)
  }

  @Test
  def rddCacheDisk(): Unit = {
    val spark: SparkSession = env.createSession()

    val df = spark.sparkContext.makeRDD(1.to(10000))
      .persist(StorageLevel.DISK_ONLY)

    val count = df.count()
    assert(count == 10000)

    assert(df.reduce(_ + _) == 50005000)
  }

  @Test
  def dockerTest(): Unit = {
    val zk = System.getProperty("shuttle.docker.zkserver");
    if (StringUtils.isEmpty(zk)) {
      return ;
    }
    val conf = new SparkConf()
    conf.set(Ors2Config.serviceRegistryZKServers, zk)
    runWithConf(conf)
  }
}