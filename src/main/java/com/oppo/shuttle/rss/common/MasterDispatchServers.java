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

import java.util.*;

public class MasterDispatchServers {

    private String dataCenter;
    private String cluster;
    private String rootDir;
    private String fsConf;
    private List<ServerDetailWithStatus> candidates;
    private List<Ors2WorkerDetail> serverDetailList;

    public MasterDispatchServers(String dataCenter,
                                 String cluster,
                                 String rootDir,
                                 List<ServerDetailWithStatus> candidates,
                                 String fsConf) {
        this.dataCenter = dataCenter;
        this.cluster = cluster;
        this.rootDir = rootDir;
        this.candidates = candidates;
        this.serverDetailList = new ArrayList<>();
        this.fsConf = fsConf;
    }

    public List<Ors2WorkerDetail> getServerDetailList() {
        return serverDetailList;
    }

    public void setServerDetailList(List<Ors2WorkerDetail> serverDetailList) {
        this.serverDetailList = serverDetailList;
    }

    public String getDataCenter() {
        return dataCenter;
    }

    public String getCluster() {
        return cluster;
    }

    public String getRootDir() {
        return rootDir;
    }

    public List<ServerDetailWithStatus> getCandidates() {
        return candidates;
    }

    public List<Ors2WorkerDetail> getCandidatesByWeight(int maxCount) {
        if (maxCount <= candidates.size()) {
            return getByWeight(maxCount);
        } else {
            return getByTimes(maxCount);
        }
    }

    public String getFsConf() {
        return fsConf;
    }

    private static class Resource {
        private final ServerDetailWithStatus server;
        private final int maxUse;
        private int alreadyUse = 0;

        public Resource(ServerDetailWithStatus server, int maxUse) {
            this.server = server;
            this.maxUse = maxUse;
        }
    }

    /**
     * Weight distribution algorithm. The minimum weight is required to be 1.
     * If the weight is too small, it may not be assigned.
     * Use the weight divided by the minimum weight to determine the number of times the shuffle worker appears.
     *
     * The improved method avoids the problem of invalid weight distribution
     * if the number of requested shuffle workers is greater than the number of clusters.
     */
    public List<Ors2WorkerDetail> getByTimes(int maxCount) {
        if (maxCount <= 0 || candidates.isEmpty()) {
            return new ArrayList<>();
        }

        int min = candidates.stream().mapToInt(ServerDetailWithStatus::getLoadWeight).min().getAsInt();

        ArrayList<Ors2WorkerDetail> res = new ArrayList<>(candidates.size());
        ArrayList<Resource> pool = new ArrayList<>(candidates.size());

        candidates.forEach(x -> {
            int maxUse = x.getLoadWeight() / min;
            pool.add(new Resource(x, maxUse));
        });

        Random random = new Random();
        while (res.size() < maxCount && !pool.isEmpty()) {
            int index = random.nextInt(pool.size());
            Resource resource = pool.get(index);
            res.add(resource.server.getServerDetail());

            resource.alreadyUse += 1;
            if (resource.alreadyUse >= resource.maxUse) {
                pool.remove(index);
            }
        }

        return res;
    }

    public List<Ors2WorkerDetail> getByWeight(int maxCount) {
        if (maxCount <= 0 || candidates.isEmpty()) {
            return new ArrayList<>();
        }

        float sum = candidates.stream().mapToInt(ServerDetailWithStatus::getLoadWeight).sum();
        float[] area = new float[candidates.size()];
        float start = 0;
        for (int i = 0; i < area.length; i++) {
            area[i] = start + candidates.get(i).getLoadWeight() / sum;
            start = area[i];
        }

        Random random = new Random();
        ArrayList<Ors2WorkerDetail> res = new ArrayList<>(maxCount);
        while (res.size() < maxCount) {
            float nextFloat = random.nextFloat();
            for (int i = 0; i < area.length; i++) {
                if (nextFloat < area[i]) {
                    res.add(candidates.get(i).getServerDetail());
                    break;
                }
            }
        }

        return res;
    }
}
