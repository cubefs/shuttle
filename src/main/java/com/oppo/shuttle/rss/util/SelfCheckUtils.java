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

package com.oppo.shuttle.rss.util;

import com.oppo.shuttle.rss.ShuffleServerConfig;
import com.oppo.shuttle.rss.common.Constants;
import com.oppo.shuttle.rss.metrics.Ors2MetricsConstants;
import com.oppo.shuttle.rss.storage.fs.FSDataOutputStream;
import com.oppo.shuttle.rss.storage.fs.FileSystem;
import com.oppo.shuttle.rss.storage.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SelfCheckUtils {

    private static final String selfCheckInfo = "self check ok!";
    private static final Logger logger = LoggerFactory.getLogger(SelfCheckUtils.class);

    public static boolean selfCheck(ShuffleServerConfig config, String localIp) {
        // check file limit
        double currentFile = Ors2MetricsConstants.partitionCurrentCount.get();
        if (currentFile >= config.getMaxOpenFiles()) {
            logger.warn("The number of open files exceeds the limit，current {}", currentFile);
            return false;
        }

        FileSystem fs = config.getStorage().getFs();
        Path dataFile = Path.of(getSelfCheckFile(config.getRootDirectory(), localIp));
        try (FSDataOutputStream writerStream = fs.create(dataFile, true)) {
            byte[] selfCheckBytes = selfCheckInfo.getBytes();
            writerStream.write(selfCheckBytes);
            logger.info("Worker storage check success, write file: {} ", dataFile);
            return true;
        } catch (Throwable t) {
            logger.error("Worker storage check fail: ", t);
            return false;
        } finally {
            try {
                fs.delete(dataFile, false);
            } catch (IOException e) {
                logger.warn("delete {}", dataFile, e);
            }
        }
    }

    private static String getSelfCheckFile(String rootDir, String localIp) {
        String localCheckPath = rootDir + "/local_self_check_" + localIp + "_" +
                System.currentTimeMillis();
        return localCheckPath + Constants.SHUFFLE_DATA_FILE_POSTFIX;
    }

}
