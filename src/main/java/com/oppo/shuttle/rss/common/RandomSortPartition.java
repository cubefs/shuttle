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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *  Disrupt the partition order to ensure that each task processes different partition data at the same time
 *  to balance the processing pressure of the shuffle worker.
 *
 * When sorting, use scrambled partition numbers.
 * When sending data, use the correct partition number
 */
public class RandomSortPartition {
    private static final Logger logger = LoggerFactory.getLogger(RandomSortPartition.class);

    private final int numberPartitions;

    private Map<Integer, Integer> sortPartition;

    private Map<Integer, Integer> restorePartition;

    public RandomSortPartition(int numberPartitions) {
        this.numberPartitions = numberPartitions;
        shuffle();
    }

    private void shuffle() {
        sortPartition = new HashMap<>(numberPartitions);
        restorePartition = new HashMap<>(numberPartitions);

        ArrayList<Integer> shuffleList = new ArrayList<>(numberPartitions);
        for (int i = 0; i < numberPartitions; i++) {
            shuffleList.add(i);
        }

        Collections.shuffle(shuffleList);

        for (int i = 0; i < numberPartitions; i++) {
            sortPartition.put(i, shuffleList.get(i));
            restorePartition.put(shuffleList.get(i), i);
        }

        if (logger.isDebugEnabled()) {
            sortPartition.forEach((x, y) -> {
                logger.debug("sort partition: {}(restore) = {}(sort)", x, y);
            });
        }
    }

    public Integer sort(Integer partitionId) {
        return sortPartition.get(partitionId);
    }

    public Integer restore(Integer partitionId) {
        return restorePartition.get(partitionId);
    }

    public int getNumberPartitions() {
        return numberPartitions;
    }

    public Map<Integer, Integer> getSortPartition() {
        return sortPartition;
    }

    public Map<Integer, Integer> getRestorePartition() {
        return restorePartition;
    }
}
