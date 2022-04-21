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

package com.oppo.shuttle.rss.metrics;

import io.prometheus.client.*;

public class Ors2MetricsConstants {
    public final static Counter writeTotalBytes = Counter.build()
            .name("write_total_bytes")
            .help("Number of bytes written to disk")
            .register();

    public static final Counter appTotalCount = Counter.build()
            .name("app_total_count")
            .help("Total number of apps")
            .register();

    public static final Gauge appCurrentCount = Gauge.build()
            .name("app_current_count")
            .help("The number of apps currently running")
            .register();

    public static final Gauge partitionCurrentCount = Gauge.build()
            .name("partition_current_count")
            .help("The number of partitions currently performing read and write operations")
            .register();

    public static final Gauge bufferedDataSize = Gauge.build()
            .name("buffered_datasize")
            .help("The amount of data currently written to the buffer, unit byte")
            .register();

    public static final Histogram dumpLatencyHistogram = Histogram.build()
            .name("dump_latency_histogram")
            .help("Disk write time distribution interval")
            .buckets(5, 10, 50, 100, 200, 500, 1000)
            .register();

    public static final Counter busyControlTimes = Counter.build()
            .name("busy_control_times")
            .help("Intercepted request for busy control")
            .register();

    public static final Counter memoryControlTimes = Counter.build()
            .name("memory_control_times")
            .help("Intercepted request for memory control")
            .register();

    public static final Counter connTotalCount = Counter.build()
            .name("conn_total_count")
            .help("Total number of connections")
            .register();

    public static final Counter connReleasedCount = Counter.build()
            .name("conn_released_count")
            .help("Number of connections released")
            .register();

    public static final Gauge workerNum = Gauge.build()
            .name("worker_num")
            .labelNames("datacenter", "cluster", "tag")
            .help("Number of workers managed by the master")
            .register();

    public static final Counter workerDistributeTimes = Counter.build()
            .name("worker_distribute_times")
            .labelNames("worker_id")
            .help("Workers distributed times")
            .register();

    public static void register() {
    }
}
