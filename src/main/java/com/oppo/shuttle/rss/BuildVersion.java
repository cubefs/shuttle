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

package com.oppo.shuttle.rss;

import com.oppo.shuttle.rss.common.Constants;
import com.oppo.shuttle.rss.exceptions.Ors2FileException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class BuildVersion {

    public static String projectVersion = Constants.UNKNOWN_BUILD_VERSION;
    public static String gitCommitVersion = Constants.UNKNOWN_BUILD_VERSION;

    static {
        try (InputStream versionFileInputStream =
                     Thread.currentThread().getContextClassLoader().getResourceAsStream(Constants.BUILD_VERSION_FILE)) {
            Properties properties = new Properties();
            properties.load(versionFileInputStream);
            projectVersion = properties.getProperty(Constants.PROJECT_VERSION_KEY);
            gitCommitVersion = properties.getProperty(Constants.GIT_COMMIT_VERSION_KEY);
        } catch (IOException e) {
            throw new Ors2FileException("Failed to open build version file " + Constants.BUILD_VERSION_FILE);
        }
    }
}
