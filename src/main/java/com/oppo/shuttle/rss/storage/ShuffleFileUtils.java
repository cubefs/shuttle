
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

package com.oppo.shuttle.rss.storage;

import com.oppo.shuttle.rss.common.StageShuffleId;

import java.nio.file.Path;
import java.nio.file.Paths;

/***
 * Utility methods for shuffle files.
 */
public class ShuffleFileUtils {
    public static String getShuffleFileName(int shuffleId, int partitionId, String hostName) {
        return String.format("shuffle_%s_%d_%d", hostName, shuffleId, partitionId);
    }
    
    public static String getShuffleFilePath(String rootDir, StageShuffleId appShuffleId, int partitionId, String hostName) {
        String fileName = getShuffleFileName(
                appShuffleId.getShuffleId(), partitionId, hostName);

        String path = Paths.get(
                getAppShuffleDir(rootDir, appShuffleId.getAppId()),
                appShuffleId.getAppAttempt(),
                String.valueOf(appShuffleId.getShuffleId()),
                String.valueOf(appShuffleId.getStageAttempt()),
                String.valueOf(partitionId),
                fileName).toString();
        return path;
    }

    public static Path getStageCompleteSignPath(String rootDir, StageShuffleId stageShuffleId) {
        Path path = Paths.get(
            getAppShuffleDir(rootDir, stageShuffleId.getAppId()),
                stageShuffleId.getAppAttempt(),
            String.valueOf(stageShuffleId.getShuffleId()),
            String.valueOf(stageShuffleId.getStageAttempt()), "_SUCCEED");
        return path;
    }

    public static Path getAppCompleteSignPath(String rootDir, String appId, String appAttempt) {
        Path path = Paths.get(
                getAppShuffleDir(rootDir, appId),
                appAttempt + "_SUCCEED");
        return path;
    }

    public static String getAppShuffleDir(String rootDir, String appId) {
        return Paths.get(rootDir, appId).toString();
    }

    public static String getShuffleFileDir(String rootDir, StageShuffleId stageShuffleId, int partitionId) {
        String path = Paths.get(
                getAppShuffleDir(rootDir, stageShuffleId.getAppId()),
                stageShuffleId.getAppAttempt(),
                String.valueOf(stageShuffleId.getShuffleId()),
                String.valueOf(stageShuffleId.getStageAttempt()),
                String.valueOf(partitionId)).toString();
        return path;
    }
}
