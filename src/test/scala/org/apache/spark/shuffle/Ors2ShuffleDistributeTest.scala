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

import com.oppo.shuttle.rss.common.{Constants, Ors2WorkerDetail, RandomSortPartition}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.Assertions.assert
import org.testng.annotations.Test

import scala.collection.JavaConversions._

class Ors2ShuffleDistributeTest {
  @Test
  def partitionRandom(): Unit = {
    val randomPartitionSort: RandomSortPartition = new RandomSortPartition(200)
    val sortPartition: java.util.Map[Integer, Integer] = randomPartitionSort.getSortPartition
    val restorePartition: java.util.Map[Integer, Integer] = randomPartitionSort.getRestorePartition
    assert(sortPartition.size == restorePartition.size)

    sortPartition.foreach(x => {
      val restore = restorePartition.get(x._2)
      assert(x._1.equals(restore))
    })
  }

  @Test
  def testDistributeServerAlgorithm(): Unit = {
    val conf = new SparkConf()
    conf.set(Ors2Config.workerCountPerGroup, 3)
    conf.setMaster("local[1]")
    conf.set(Ors2Config.serviceManagerType, Constants.MANAGER_TYPE_ZK)
    SparkSession.builder().config(conf).getOrCreate()
    val ors2ShuffleManager = new Ors2ShuffleManager(conf)

    val servers = Array(
      "localhost1:19190:19191",
      "localhost2:19190:19191",
      "localhost3:19190:19191"
    ).map(Ors2WorkerDetail.fromSimpleString)

    val (partitionMapToShuffleWorkers, _) =
      ors2ShuffleManager.distributeShuffleWorkersToPartition(
        1, 4000, servers)
    println(partitionMapToShuffleWorkers)

    val assign = partitionMapToShuffleWorkers.toSeq
      .map(x => (x._2, 1))
      .groupBy(x => x._1)
      .map(x => (x._1, x._2.length))

    assign.foreach(println)

    assert(assign.size == servers.length)

    val min = assign.values.min
    val max = assign.values.max

    assert(max - min <= 1 && max - min >= 0)
  }

}
