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


import java.util.List;

public class ConfiguredServerList {

    private List<Ors2WorkerDetail> ors2WorkerDetailList;
    private String conf;
    private String rootDir;
    private String dataCenter;
    private String cluster;

    public ConfiguredServerList(List<Ors2WorkerDetail> ors2WorkerDetailList,
                                String conf,
                                String rootDir,
                                String dataCenter,
                                String cluster) {
        this.ors2WorkerDetailList = ors2WorkerDetailList;
        this.conf = conf;
        this.rootDir = rootDir;
        this.dataCenter = dataCenter;
        this.cluster = cluster;
    }

    public List<Ors2WorkerDetail> getServerDetailList() {
        return ors2WorkerDetailList;
    }

    public Ors2WorkerDetail[] getServerDetailArray() {
        return ors2WorkerDetailList.toArray(new Ors2WorkerDetail[] {});
    }


    public String getConf() {
        return conf;
    }

    public String getRootDir() {
        return rootDir;
    }

    public String getDataCenter() {
        return dataCenter;
    }

    public String getCluster() {
        return cluster;
    }

    @Override
    public String toString() {
        return "ConfiguredServerList{" +
                "serverDetailList=" + ors2WorkerDetailList +
                ", conf='" + conf + '\'' +
                ", rootDir='" + rootDir + '\'' +
                ", dataCenter='" + dataCenter + '\'' +
                ", cluster='" + cluster + '\'' +
                '}';
    }
}
