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

import java.util.concurrent.TimeUnit
import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}
import org.apache.spark.network.util.ByteUnit

object Ors2Config {
  val dataCenter: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.rss.dataCenter")
      .doc("data center for RSS cluster. If not specified, will try to get value from the environment.")
      .stringConf
      .createWithDefault("default_dc")

  val cluster: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.rss.cluster")
      .doc("RSS cluster name.")
      .stringConf
      .createWithDefault("default_cluster")

  val masterName: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.rss.masterName")
      .doc("RSS master name.")
      .stringConf
      .createWithDefault("default_master")

  val serviceManagerType: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.rss.serviceManager.type")
      .doc("type of service manage to use: zookeeper, master.")
      .stringConf
      .createWithDefault("master")

  val serviceRegistryZKServers: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.rss.serviceRegistry.zookeeper.servers")
      .doc("ZooKeeper host:port addresses. Specify more than one as a comma-separated string.")
      .stringConf
      .createWithDefault("")

  val dfsDirPrefix: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.rss.dfs.dirPrefix")
      .doc("Shuffle data and index file directory for dfs")
      .stringConf
      .createWithDefault("/rss/data")

  val useEpoll: ConfigEntry[Boolean] =
    ConfigBuilder("spark.shuffle.rss.useEpoll")
      .doc("Whether use epoll eventloop or not.")
      .booleanConf
      .createWithDefault(false)

  val writerBufferSize: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.writer.bufferSize")
      .doc("Internal buffer size for shuffle writer.")
      .intConf
      .createWithDefault(1024 * 1024)

  val writerBufferSpill: ConfigEntry[Long] =
    ConfigBuilder("spark.shuffle.rss.writer.bufferSpill")
      .doc("The threshold for shuffle writer buffer to spill data. Here spill means removing the data out of the buffer " +
        "and send to shuffle server. Note: the config only affects one executor")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("128mb")

  val writeBlockSize: ConfigEntry[Long] =
    ConfigBuilder("spark.shuffle.rss.writer.blockSize")
      .doc("Set the block size of the writer. If the shuffle data of a partition exceeds this value, it will be sent to the shuffle worker. " +
        "In fact, it also limits the size of a network request.Default: 1MB。")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1mb")

  val minWriteBlockSize: ConfigEntry[Long] =
    ConfigBuilder("spark.shuffle.rss.writer.min.blockSize")
      .doc("Set the min block size of the writer. Limit the floor of writeBlockSize config")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("256kb")

  val maxWriteBlockSize: ConfigEntry[Long] =
    ConfigBuilder("spark.shuffle.rss.writer.max.blockSize")
      .doc("Set the max block size of the writer. Limit the ceil of writeBlockSize config")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("4mb")

  val writerMaxRequestSize: ConfigEntry[Long] =
    ConfigBuilder("spark.shuffle.rss.writer.maxRequestSize")
      .doc("The maximum amount of data per network request. Multiple blocks may be combined to send a single request to the server.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("2mb")

  val maxTotalBufferDataSize: ConfigEntry[Long] =
    ConfigBuilder("spark.shuffle.rss.writer.maxTotalBufferDataSize")
      .doc("The maximum amount of data for total buffered data to all shuffle worker.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("100mb")

  val maxFlyingPackageNum: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.writer.maxFlyingPackageNum")
      .doc("The maximum amount of data for total buffered data to all shuffle worker.")
      .intConf
      .createWithDefaultString("16")

  val retryBaseWaitTime =
    ConfigBuilder("spark.shuffle.rss.retry.baseWaitTime")
      .doc("Network error, flow control retry interval. Default 1s.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("100ms")

  val networkTimeout =
    ConfigBuilder("spark.shuffle.rss.network.timeout")
      .doc("Network timeout time, including connection time, read and write time. Default 600s")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("300s")

  val networkSlowTime =
    ConfigBuilder("spark.shuffle.rss.network.slowTime")
      .doc("Network operation slow log time. Default 10s")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("10s")

  val ioThreads =
    ConfigBuilder("spark.shuffle.rss.network.ioThreads")
      .doc("Network IO threads. Default 2")
      .intConf
      .createWithDefault(2)

  val numConnections = ConfigBuilder("spark.shuffle.rss.network.numConnections")
    .doc("Network connections. Default 1")
    .intConf
    .createWithDefault(1)

  //One writer buffer per partition
  val SHUFFLE_WRITER_BYPASS = "bypass"

  //The partition shares the buffer, and the spill data is sorted out of the partition heap
  val SHUFFLE_WRITER_UNSAFE = "unsafe"

  // Sort according to the partition and key, and save the aggregation results on the map side
  val SHUFFLE_WRITER_SORT = "sort"

  // Automatic selection according to conditions
  val SHUFFLE_WRITER_AUTO = "auto"

  val shuffleWriterType: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.rss.writer.type")
      .doc("Using the shuffle writer type")
      .stringConf
      .createWithDefault(SHUFFLE_WRITER_AUTO)

  val writerAsyncFinish: ConfigEntry[Boolean] =
    ConfigBuilder("spark.shuffle.rss.writer.async")
      .doc("whether use async mode for writer to finish uploading data.")
      .booleanConf
      .createWithDefault(true)

  val getClientMaxRetries: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.get.client.max.retries")
      .doc("Max retries to get idle client from the client factory")
      .intConf
      .createWithDefault(5)

  val getClientWaitTime =
    ConfigBuilder("spark.shuffle.rss.get.client.wait.time")
      .doc("When get idle client encounters error, wait this time and retry")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("1s")

  val dataInputReadyQueryInterval: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.inputReady.queryInterval")
      .doc("Query shuffle data input ready interval (milliseconds) e.g. whether a map task's data finished.")
      .intConf
      .createWithDefault(100)

  val maxRequestShuffleWorkerCount: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.maxRequestWorkerCount")
      .doc("Max remote shuffle worker used for this application.")
      .intConf
      .createWithDefault(2048)

  val minRequestShuffleWorkerCount: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.minRequestWorkerCount")
      .doc("Min remote shuffle worker used for this application.")
      .intConf
      .createWithDefault(3)

  val partitionCountPerShuffleWorker: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.partitionCountPerShuffleWorker")
      .doc("Partition count mapping to one shuffle worker.")
      .intConf
      .createWithDefault(5)

  val workerCountPerGroup: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.shuffleWorkerCountPerGroup")
      .doc("Shuffle worker count per group, sender send one partition data to single workers in same group")
      .intConf
      .createWithDefault(3)

  val workerMaxPerGroup: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.workerMaxPerGroup")
      .doc("The maximum number of workers used in each partition")
      .intConf
      .createWithDefault(10)

  val workerCoresPerGroup: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.workerCoresPerGroup")
      .doc("The maximum number of workers used in each partition")
      .intConf
      .createWithDefault(1000)

  val mapWriteDispersion: ConfigEntry[Boolean] =
    ConfigBuilder("spark.shuffle.rss.mapWriteDispersion")
      .doc("Whether map shuffle data is written to different workers")
      .booleanConf
      .createWithDefault(false)

  val sendDataMaxRetries: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.sender.max.retries")
    .doc("Maximum number of retries when sending data to server before giving up.")
    .intConf
    .createWithDefault(120)

  val workerRetryNumber: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.write.workerRetryNumber")
      .doc("Send flow control request, the number of times a worker tries to retry. Default 5")
      .intConf
      .createWithDefault(3)

  val ioMaxRetry: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.io.maxRetries")
      .doc("IO exception, number of retries. Default 2")
      .intConf
      .createWithDefault(2)

  val ioErrorResend: ConfigEntry[Boolean] =
    ConfigBuilder("spark.shuffle.rss.io.errorResend")
      .doc("Whether to resend the request if the io is abnormal. Default true")
      .booleanConf
      .createWithDefault(false)

  val ioRetryWait: ConfigEntry[Long] =
    ConfigBuilder("spark.shuffle.rss.io.ioRetryWait")
      .doc("IO retry waiting time, default 30s")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("30s")

  val ShuffleVersion: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.version")
      .doc("rss shuffle version")
      .intConf
      .createWithDefault(0)

  val jobPriority: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.job.priority")
      .doc("The priority of the job")
      .intConf
      .createWithDefault(1)

  /************************** for test ***********************/
  val mockReadErrorProbability: ConfigEntry[Double] =
    ConfigBuilder("spark.shuffle.rss.reader.mockErrorProbability")
      .doc("The probability of mock error in shuffle reader")
      .doubleConf
      .createWithDefault(0.0)

  // file read config
  val readBufferSize: ConfigEntry[Long] =
    ConfigBuilder("spark.shuffle.rss.read.bufferSize")
      .doc("The size of the read buffer. Default 64k")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("64k")

  val shuffleSpillMemoryThreshold: ConfigEntry[Long] =
    ConfigBuilder("spark.shuffle.rss.memory.threshold")
      .doc("Maximum memory limit for shuffle overflow")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("512mb")

  val shuffleReadIOThreads: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.read.io.threads")
      .doc("Number of read threads")
      .intConf
      .createWithDefault(2)
      .asInstanceOf[ConfigEntry[Int]]

  val shuffleReadMaxSize: ConfigEntry[Long] =
    ConfigBuilder("spark.shuffle.rss.read.max.size")
      .doc("The maximum size of the read buffer.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("128mb")

  val shuffleReadMergeSize: ConfigEntry[Long] =
    ConfigBuilder("spark.shuffle.rss.read.merge.size")
      .doc("Combining the read multiple blocks and providing them to the iterator as" +
        " a whole has reduced the synchronization queue overhead. Default 32m")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("32mb")

  val shuffleReadType: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.rss.read.type")
      .doc("Read mode. In asynchronous mode, one or more threads will be started to read;" +
        "and in synchronous mode, read in the main thread of the reader")
      .stringConf
      .createWithDefault("async")

  val shuffleReadWaitFinalizeTimeout: ConfigEntry[Long] =
    ConfigBuilder("spark.shuffle.rss.read.waitFinalizeTimeout")
      .doc("The maximum timeout time to wait for shuffle write execution. Default 600s" )
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("600s")

  val deleteShuffleDir: ConfigEntry[Boolean] =
    ConfigBuilder("spark.shuffle.rss.deleteShuffleDir")
      .doc("After the app finishes running, release the automatically deleted data directory. The default is true")
      .booleanConf
      .createWithDefault(true)

  val flowControlEnable: ConfigEntry[Boolean] =
    ConfigBuilder("spark.shuffle.rss.flowControlEnable")
      .doc("Whether to enable flow control. Defaults to true")
      .booleanConf
      .createWithDefault(true)

  val SHUFFLE_TYPE_ASYNC = "async"
  val SHUFFLE_TYPE_SYNC = "sync"

  val isGetActiveMasterFromZk: ConfigEntry[Boolean] =
    ConfigBuilder("spark.shuffle.rss.isGetActiveMasterFromZk")
      .doc("Whether to use the value of the active cluster from Zookeeper. The default is true")
      .booleanConf
      .createWithDefault(true)

  val dagId: ConfigEntry[String] =
    ConfigBuilder("spark.oflow.dag.id")
      .doc("oflow dag id.")
      .stringConf
      .createWithDefault("")

  val shuffleWorkerDataSpillMemoryThreshold: ConfigEntry[Long] =
    ConfigBuilder("spark.shuffle.rss.workerdata.memory.threshold")
      .doc("Maximum memory limit for per worker data shuffle overflow")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1mb")

  val taskId: ConfigEntry[String] =
    ConfigBuilder("spark.oflow.task.id")
      .doc("oflow task id.")
      .stringConf
      .createWithDefault("")
}
