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
import org.apache.spark.shuffle.ors2.Ors2SparkListener
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{count, sum}
import org.testng.annotations.{AfterClass, AfterMethod, Test}

/**
 *
 */
class Ors2ShuffleClusterTest {
  val env: Ors2ShuffleTestEnv = Ors2ShuffleTestEnv.masterService()
  env.startCluster()

  @AfterMethod
  def afterMethod(): Unit = {
    Ors2SparkListener.clearListenerInstance()

    SparkSession.getActiveSession.foreach(_.stop())
  }

  @AfterClass
  def afterClass(): Unit = {
    env.stopCluster()
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

  @Test
  def testMaster(): Unit = {
    val conf = new SparkConf()
    conf.set(Ors2Config.serviceManagerType, Constants.MANAGER_TYPE_MASTER)
    val spark = env.createSession(conf)

    import spark.implicits._
    val df = 1.to(10000).toDF("value")
      .repartition(10)
      .agg(
        count("value") as "count",
        sum("value") as "sum"
      )

    checkMethod(df.take(1))
  }

  @Test
  def testZk(): Unit = {
    val conf = new SparkConf()
    conf.set(Ors2Config.serviceManagerType, Constants.MANAGER_TYPE_ZK)
    val spark = env.createSession(conf)

    import spark.implicits._
    val df = 1.to(10000).toDF("value")
      .repartition(10)
      .agg(
        count("value") as "count",
        sum("value") as "sum"
      )

    checkMethod(df.take(1))
  }
}
