package org.apache.spark.shuffle

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

class ShuttleOnYarn(val sparkConf: SparkConf, val app: ApplicationAttemptId) extends Logging {
  val applicationId: String = app.getApplicationId.toString
  val appAttemptId: Int = app.getAttemptId

  var manager: Ors2ShuffleManager = new Ors2ShuffleManager(sparkConf)

  def checkShuttleStatus(): Unit = {
    try {
      _checkShuttleStatus()
    } finally {
      manager.stop()
    }
  }

  private[this] def _checkShuttleStatus(): Unit = this.synchronized {
    val isShuttleShuffle = sparkConf.get("spark.shuffle.manager", "sort")
      .startsWith("org.apache.spark.shuffle.Ors")
    if (!isShuttleShuffle) {
      return
    }

    // First check if the shuttle service has been shut down
    if (!manager.getOrCreateServiceManager.checkShuttleIsEnable()) {
      setSortShuffle()
      log.warn("Shuttle shuffle manager closed, switch to sort shuffle manager")
      return
    }

    // Second check, whether the task is in the whitelist
    if (!whitelistCheck) {
      setSortShuffle()
      return
    }

    // Third check, if the task fails multiple times

    val changeMaxRetry = sparkConf.getInt("spark.shuffle.rss.failMaxRetry", 2)
    val oflowAttemptId = sparkConf.getInt("spark.oflow.retry.num", appAttemptId)

    if (oflowAttemptId > changeMaxRetry) {
      setSortShuffle()
      log.warn(s"Shuttle shuffle manager failed to execute(retry $oflowAttemptId), switch to sort shuffle manager")
    } else {
      log.info("Shuttle shuffle manager is in normal state and can be used")
    }
  }

  private[this] def setSortShuffle() {
    System.setProperty("spark.shuffle.manager", "sort")
  }

  /**
   * Check in advance, if the shuffle worker fails to be obtained here,
   * it will switch to yarn shuffle
   */
  def whitelistCheck(): Boolean = {
    try {
      manager.getShuffleWorkers(-1, applicationId)
      true
    } catch {
      case e: Throwable =>
        log.warn("Shuttle shuffle manager whitelist check failed, switch to sort shuffle manager", e)
        false
    }
  }
}
