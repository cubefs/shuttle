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

import com.oppo.shuttle.rss.util.ScheduledThreadPoolUtils;
import com.oppo.shuttle.rss.util.SimilarityUtils;
import io.netty.util.internal.ConcurrentSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.directory.api.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class ApplicationRequestController {

    private static final Logger logger = LoggerFactory.getLogger(ApplicationRequestController.class);

    private final Map<String, ResourceHolder> appMap = new ConcurrentHashMap<>(10);

    private final long appControlInterval;
    private final int resourceNum;
    private final int appNamePreLen;
    private final String filterExcludes;
    private final String[] excludePrefixes;

    private final long updateDelay;
    private static final long WAIT_RESOURCE_TIMEOUT = 10L;
    private static final String FILTER_SEPARATOR = ",";

    public ApplicationRequestController(
            int resourceNum,
            long appControlInterval,
            long updateDelay,
            int appNamePreLen,
            String filterExcludes) {
        this.resourceNum = resourceNum;
        this.appControlInterval = appControlInterval;
        this.updateDelay = updateDelay;
        this.filterExcludes = filterExcludes;
        this.appNamePreLen = appNamePreLen;
        excludePrefixes = parseFilterExcludes();
        updateStart();
    }

    public void updateStart() {
        ScheduledThreadPoolUtils.scheduleAtFixedRate(
                this::clearAppMap,
                updateDelay,
                appControlInterval);
    }

    private void clearAppMap() {
        long currentTimeMillis = System.currentTimeMillis();
        if (appMap.size() > 0) {
            for (Map.Entry<String, ResourceHolder> appResource : appMap.entrySet()) {
                if (appResource.getValue().isResourceExpired(currentTimeMillis, appControlInterval)) {
                    appMap.remove(appResource.getKey());
                }
            }
        }
    }

    public boolean requestCome(String appName, String appId) {
        for (String excludePrefix : excludePrefixes) {
            if (appName.startsWith(excludePrefix.trim())) {
                return true;
            }
        }
        String appSpace = getMatching(appName);
        ResourceHolder resourceHolder =
                appSpace == null ?
                        appMap.computeIfAbsent(appName, t -> new ResourceHolder(resourceNum))
                        : appMap.get(appSpace);

        if (resourceHolder.holderList.contains(appId)) {
            return true;
        } else {
            Semaphore resource = resourceHolder.semaphore;
            try {
                logger.info("Current resource holders are: {}", resourceHolder.getHolderList());
                if (resource.tryAcquire(WAIT_RESOURCE_TIMEOUT, TimeUnit.SECONDS)) {
                    resourceHolder.holderList.add(appId);
                    return true;
                }
                logger.info("{} request can't get resource in {} seconds, return fail.", appId, WAIT_RESOURCE_TIMEOUT);
                return false;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Request can't get resource in {} seconds", WAIT_RESOURCE_TIMEOUT);
                return false;
            }
        }
    }

    private String[] parseFilterExcludes() {
        if (!Strings.isEmpty(filterExcludes)) {
            if (filterExcludes.contains(FILTER_SEPARATOR)) {
                return StringUtils.split(filterExcludes, FILTER_SEPARATOR);
            } else {
                return new String[]{filterExcludes};
            }
        } else {
            return new String[0];
        }
    }

    private String getMatching(String appName){
        for (String as : appMap.keySet()) {
            if (SimilarityUtils.judgeSimilarity(as, appName, appNamePreLen)) {
                return as;
            }
        }
        return null;
    }

    static class ResourceHolder {
        private final Set<String> holderList;
        private final Semaphore semaphore;
        private final long startTimeMillis;

        public ResourceHolder(int resourceNum) {
            this.semaphore = new Semaphore(resourceNum, true);
            startTimeMillis = System.currentTimeMillis();
            holderList = new ConcurrentSet<>();
        }

        public boolean isResourceExpired(
                long currentTimeMillis,
                long appControlInterval) {
            return currentTimeMillis - startTimeMillis > appControlInterval;
        }

        public Set<String> getHolderList() {
            return holderList;
        }
    }
}
