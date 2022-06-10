package org.apache.spark.shuffle.ors2

import org.apache.spark.executor.{ShuffleReadMetrics, ShuffleWriteMetrics}
import org.apache.spark.shuffle.{ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter}

case class Ors2ShuffleReadMetrics(reporter: ShuffleReadMetricsReporter) extends ShuffleReadMetrics {
  override private[spark] def incRemoteBytesRead(v: Long): Unit = {
    reporter.incRemoteBytesRead(v)
  }

  override def incFetchWaitTime(v: Long): Unit = {
    reporter.incFetchWaitTime(v)
  }

  override private[spark] def incRecordsRead(v: Long):Unit = {
    reporter.incRecordsRead(v)
  }
}

case class Ors2ShuffleWriteMetrics(reporter: ShuffleWriteMetricsReporter) extends ShuffleWriteMetrics {
  override private[spark] def incBytesWritten(v: Long): Unit = {
    reporter.incBytesWritten(v)
  }

  override private[spark] def incRecordsWritten(v: Long): Unit = {
    reporter.incRecordsWritten(v)
  }

  override private[spark] def incWriteTime(v: Long): Unit = {
    reporter.incWriteTime(v)
  }

}

