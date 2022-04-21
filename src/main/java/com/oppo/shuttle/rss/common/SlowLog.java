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

public class SlowLog {
    private static final Logger logger = LoggerFactory.getLogger(SlowLog.class);

    private long start = System.currentTimeMillis();

    public void reset() {
        start = System.currentTimeMillis();
    }

    public void log(String mark, int max) {
        long diff = System.currentTimeMillis() - start;
        if (diff >= max) {
            logger.warn("{} so slow, cost {}ms", mark, diff);
        }
    }
}
