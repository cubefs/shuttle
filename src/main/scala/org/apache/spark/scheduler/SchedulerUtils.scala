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

package org.apache.spark.scheduler

import org.apache.spark.internal.config._
import org.apache.spark.shuffle.Ors2Config
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}

object SchedulerUtils {
  def conf: SparkConf = SparkEnv.get.conf
  def env: SparkEnv = SparkEnv.get
  def context: SparkContext = SparkContext.getActive.get

  def getStageId(shuffleId: Int): Int = {
    context.dagScheduler.shuffleIdToMapStage.get(shuffleId) match {
      case Some(stage) => stage.id
      case None => -1
    }
  }

  def getShuffleId(stageId: Int): Int = {
    context.dagScheduler.shuffleIdToMapStage.find(_._2.id == stageId) match {
      case Some(stage) => stage._1
      case None => -1
    }
  }

  def getWorkerGroupSize(shuffleId: Int): Int = {
    val maybeStage = context.dagScheduler.shuffleIdToMapStage.get(shuffleId)
    val preGroupSize = Math.max(1, conf.get(Ors2Config.workerCountPerGroup))
    val maxGroupSize = conf.get(Ors2Config.workerMaxPerGroup)
    val coreGroupSize = conf.get(Ors2Config.workerCoresPerGroup)

    if (maybeStage.isEmpty) {
      return preGroupSize
    }

    val numMaps = maybeStage.get.shuffleDep.rdd.getNumPartitions

    val maxExecutors: Int = if (conf.getBoolean("spark.dynamicAllocation.enabled", false)) {
      conf.getInt(DYN_ALLOCATION_MAX_EXECUTORS.key, 1)
    } else {
     conf.getInt(EXECUTOR_INSTANCES.key, 1)
    }

    val maxCores = maxExecutors * conf.getInt("spark.executor.cores", 1)

    val groupSize = Math.max(Math.min(numMaps, maxCores) / coreGroupSize, preGroupSize)

    Math.min(maxGroupSize, groupSize)
  }
}
