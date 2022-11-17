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

import com.oppo.shuttle.rss.util.ConfUtil
import org.apache.hadoop.yarn.api.records.{ApplicationAttemptId, ApplicationId}
import org.testng.annotations.{BeforeClass, Test}

class ShuttleOnYarnTest {
  System.setProperty(ConfUtil.RSS_CONF_DIR_PROP, "D:\\code\\shuttle\\conf")

  private var env: Ors2ShuffleTestEnv = _
  private val app = ApplicationAttemptId.newInstance(ApplicationId.newInstance(System.currentTimeMillis(), 1), 1)

  @BeforeClass
  def startEnv(): Unit = {
    env = Ors2ShuffleTestEnv.masterService()
    env.startCluster()
  }

  @Test
  def sortShuffle(): Unit = {
    val conf = env.sparkConf()
      .set("spark.shuffle.manager", "sort")
    new ShuttleOnYarn(conf, app).checkShuttleStatus()
  }

  @Test
  def whitelistCheckFailTest(): Unit = {
    val conf = env.sparkConf().set("spark.app.id", app.getApplicationId.toString)
    new ShuttleOnYarn(conf, app).checkShuttleStatus()
    assert(System.getProperty("spark.shuffle.manager") == "sort")
  }

  @Test
  def whitelistCheckSuccessTest(): Unit = {
    val conf = env.sparkConf().set("spark.app.id", app.getApplicationId.toString)
      .set(Ors2Config.taskId, "task-test")
    new ShuttleOnYarn(conf, app).checkShuttleStatus()
    assert(System.getProperty("spark.shuffle.manager") != "sort")
  }
}
