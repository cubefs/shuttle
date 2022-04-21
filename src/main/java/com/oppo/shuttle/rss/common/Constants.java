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

package com.oppo.shuttle.rss.common;

import java.util.concurrent.TimeUnit;

public class Constants {
  public static final int CHECK_SUM_SEQID  = -1;
  public static final int SHUFFLE_DATA_DUMP_THRESHOLD = 1 * 1024 *1024;
  public static final int SHUFFLE_INDEX_COUNT_DUMP_THRESHOLD = 16 * 1024;
  public static final int SHUFFLE_DATA_DUMPER_THREADS = 16;
  public static final int STAGE_FINALIZED_CHECK_INTERVAL_MILLIS = 1000;
  public static final long EMPTY_CHECKSUM_DEFAULT = -1L;
  public static final int SHUFFLE_STAGE_INDEX_COUNT_DUMP_THRESHOLD = 1024 * 1024;
  public static final int SHUFFLE_PARTITION_INDEX_COUNT_DUMP_THRESHOLD = 16 * 1024;
  public static final int SHUFFLE_PARTITION_COUNT_DEFAULT = 200;
  public static final long SHUFFLE_GET_RESULT_TIMEOUT = 15L;

  public static final String DEFAULT_SUCCESS = "success";
  public static final String SHUFFLE_FILE_PREFIX = "shuffle_";
  public static final String SHUFFLE_DATA_FILE_POSTFIX = ".dat";
  public static final String SHUFFLE_INDEX_FILE_POSTFIX = ".idx";
  public static final String SHUFFLE_FINAL_DATA_FILE_POSTFIX = ".dat_final";
  public static final String SHUFFLE_FINAL_INDEX_FILE_POSTFIX = ".idx_final";
  public static final String SHUFFLE_FINAL_FILE_POSTFIX = "_final";
  public static final String SHUFFLE_DECODER_NAME = "shuffle-decoder";
  public static final String BUILD_CONN_HANDLER_NAME = "build-connection-handler";

  /**
   * Build version constants config
   */
  public static final String UNKNOWN_BUILD_VERSION = "<UNKNOWN_VERSION>";
  public static final String BUILD_VERSION_FILE = "rss-build-version.properties";
  public static final String PROJECT_VERSION_KEY = "project_version";
  public static final String GIT_COMMIT_VERSION_KEY = "git_commit_version";

  /**
   * shuffle server constant config
   */
  public static final String DATA_CENTER_DEFAULT = "default";
  public static final String CLUSTER_DEFAULT = "default";
  public static final String MASTER_NAME_DEFAULT = "default_master";
  public static final String MANAGER_TYPE_ZK = "zookeeper";
  public static final String MANAGER_TYPE_MASTER = "master";

  public static final int SERVER_SHUTDOWN_PRIORITY = 1000;
  public static final int MASTER_HTTP_SERVER_THREADS = 1;
  public static final int MASTER_WORKER_STATUS_UPDATE_DELAY_MS = 30000;
  public static final long SERVER_CONNECTION_IDLE_TIMEOUT_MILLIS_DEFAULT = 2 * 60 * 60 * 1000L;
  public static final long CLI_CONN_IDLE_TIMEOUT_MS = 3 * 60 * 1000L;

  public static final long APP_OBJ_RETENTION_MILLIS_DEFAULT = TimeUnit.HOURS.toMillis(6);
  public static final long APP_STORAGE_RETENTION_MILLIS_DEFAULT = TimeUnit.HOURS.toMillis(24);

  /**
   * test constant config
   */
  public static final String TEST_DATACENTER_DEFAULT = "default_dc";
  public static final String TEST_CLUSTER_DEFAULT = "default_cluster";


}
