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

package com.oppo.shuttle.rss.server.master;

import com.oppo.shuttle.rss.common.Ors2FilesystemConf;
import com.oppo.shuttle.rss.common.ServerListDir;
import com.oppo.shuttle.rss.exceptions.Ors2Exception;
import com.oppo.shuttle.rss.storage.ShuffleFileStorage;
import com.oppo.shuttle.rss.storage.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *  The spark driver will clean up the shuffle data directory after the application ends.
 *  If the task exits abnormally, the data directory will not be cleaned up normally,
 *  and the master will be responsible for cleaning up these leftover directories.
 */
public class ShuffleDataDirClear {
    private static final Logger logger = LoggerFactory.getLogger(ShuffleDataDirClear.class);

    public static final long DEFAULT_APP_FILE_RETENTION_MILLIS = TimeUnit.HOURS.toMillis(24);

    private long appFileRetentionMillis = DEFAULT_APP_FILE_RETENTION_MILLIS;

    private final Map<String, ShuffleFileStorage> storageMap = new HashMap<>();

    public ShuffleDataDirClear(long appFileRetentionMillis) {
        this.appFileRetentionMillis = appFileRetentionMillis;
    }

    public ShuffleDataDirClear() {}

    private synchronized ShuffleFileStorage getStorage(ServerListDir listDir) {
        if (!storageMap.containsKey(listDir.getRootDir())) {
            try {
                Ors2FilesystemConf conf = Ors2FilesystemConf.parseJsonString(listDir.getFsConf());
                storageMap.put(listDir.getRootDir(), new ShuffleFileStorage(listDir.getRootDir(), conf));
            } catch (Exception e) {
                logger.error("Failed to create ShuffleFileStorage", e);
            }

        }
        return storageMap.get(listDir.getRootDir());
    }

    public void execute(ServerListDir listDir) {
        ShuffleFileStorage storage = getStorage(listDir);
        if (storage == null) {
            throw new Ors2Exception("No available storage found for " + listDir.getRootDir());
        }

        deleteExpiredDir(storage, storage.getRootDir(), appFileRetentionMillis);
    }

    private void deleteExpiredDir(ShuffleFileStorage storage, String checkDir, long retentionMillis) {
        List<FileStatus> fileStatuses = storage.listStatus(checkDir);

        for (FileStatus elm : fileStatuses) {
            if (System.currentTimeMillis() >= elm.getModificationTime() + retentionMillis) {
                long start = System.currentTimeMillis();

                try {
                    storage.deleteDirectory(elm.getPath().getPath());

                    LocalDateTime modifyDataTime = LocalDateTime.ofInstant(
                            Instant.ofEpochMilli(elm.getModificationTime()),  ZoneId.systemDefault());
                    logger.info("Delete expired file or directory {} successfully, last modification {}, cost {} mills",
                            elm.getPath(), modifyDataTime, System.currentTimeMillis() - start);
                } catch (Exception e) {
                    logger.warn("{} delete failed", elm.getPath(), e);
                }
            }
        }
    }
}