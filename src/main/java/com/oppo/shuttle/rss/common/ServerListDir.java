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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@JsonIgnoreProperties("fsConf")
public class ServerListDir {

    private final String rootDir;
    private final String fsConf;
    private final Map<String, ServerDetailWithStatus> hostStatusMap = new ConcurrentHashMap<>();

    public ServerListDir(String rootDir, String fsConf) {
        this.rootDir = rootDir;
        this.fsConf = fsConf;
    }

    public String getRootDir() {
        return rootDir;
    }

    public Map<String, ServerDetailWithStatus> getHostStatusMap() {
        return hostStatusMap;
    }

    public String getFsConf() {
        return fsConf;
    }

    @Override
    public String toString() {
        return "rootDir='" + rootDir + "'\n" +
                hostStatusMap.values().stream()
                        .map(ServerDetailWithStatus::toString)
                        .collect(Collectors.joining(System.lineSeparator() + "----------" + System.lineSeparator()));
    }
}
