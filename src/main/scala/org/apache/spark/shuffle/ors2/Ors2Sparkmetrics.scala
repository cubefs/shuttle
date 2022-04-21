package org.apache.spark.shuffle.ors2

import org.apache.spark.executor.{ShuffleReadMetrics, ShuffleWriteMetrics}
import org.apache.spark.shuffle.{ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter}

case class Ors2ShuffleReadMetrics(reporter: ShuffleReadMetricsReporter) extends ShuffleReadMetrics {

  override private[spark] def incRemoteBlocksFetched(v: Long): Unit = {
    reporter.incRemoteBlocksFetched(v)
  }

  override private[spark] def incLocalBlocksFetched(v: Long): Unit = {
    reporter.incLocalBlocksFetched(v)
  }

  override private[spark] def incRemoteBytesRead(v: Long): Unit = {
    reporter.incRemoteBytesRead(v)
  }

  override private[spark] def incRemoteBytesReadToDisk(v: Long): Unit = {
    reporter.incRemoteBytesReadToDisk(v)
  }

  override private[spark] def incLocalBytesRead(v: Long): Unit = {
    reporter.incLocalBytesRead(v)
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

  override private[spark] def decBytesWritten(v: Long): Unit = {
    reporter.decBytesWritten(v)
  }

  override private[spark] def decRecordsWritten(v: Long): Unit = {
    reporter.decRecordsWritten(v)
  }
}

