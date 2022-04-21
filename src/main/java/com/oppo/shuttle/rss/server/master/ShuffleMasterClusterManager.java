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

import com.oppo.shuttle.rss.exceptions.Ors2Exception;
import com.oppo.shuttle.rss.metadata.ZkShuffleServiceManager;
import com.oppo.shuttle.rss.util.JsonUtils;
import com.oppo.shuttle.rss.common.Pair;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ShuffleMasterClusterManager implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ShuffleMasterClusterManager.class);

    private static final Map<String, Pair<String, String>> dagCluster = new ConcurrentHashMap<>();
    private ZkShuffleServiceManager serviceRegistry;
    private TreeCache treeCache;

    public ShuffleMasterClusterManager(ZkShuffleServiceManager serviceRegistry) {
        this.serviceRegistry = serviceRegistry;
        this.treeCache = serviceRegistry.getTreeCache(serviceRegistry.getDagRoot());
        try {
            init();
            treeCache.start();
            treeCache.getListenable().addListener((client, event) -> init());
        } catch (Exception e) {
            logger.error("Start tree cache error. ", e);
        }
    }

    public void init() {
        dagCluster.clear();
        List<String> nodeChildren = serviceRegistry.getNodeChildren(serviceRegistry.getDagRoot());
        if (nodeChildren != null) {
            for (String dataCenter : nodeChildren) {
                List<String> centerChildren =
                        serviceRegistry.getNodeChildren(serviceRegistry.getDagRoot() + "/" + dataCenter);
                if (centerChildren != null) {
                    for (String cluster : centerChildren) {
                        byte[] data;
                        try {
                            data = serviceRegistry.getZkNodeData(serviceRegistry.getZkDagIdPath(dataCenter, cluster));
                        } catch (Ors2Exception re) {
                            continue;
                        }
                        String nodeString = new String(data);
                        List<String> dagIdList = parseFromJson(nodeString);
                        for (String dagId : dagIdList) {
                            dagCluster.put(dagId, new Pair<>(dataCenter, cluster));
                        }
                    }
                }
            }
        }
        logger.info("Current dag cluster map is {}", dagCluster);
    }

    private static List<String> parseFromJson(String nodeString){
        Iterator<JsonNode> nodeIterator = JsonUtils.jsonToObj(nodeString).iterator();
        List<String> result = new ArrayList<>();
        while (nodeIterator.hasNext()){
            result.add(nodeIterator.next().asText());
        }
        return result;
    }

    public static Map<String, Pair<String, String>> getDagCluster() {
        return dagCluster;
    }

    @Override
    public void close() throws IOException {
        treeCache.close();
    }
}
