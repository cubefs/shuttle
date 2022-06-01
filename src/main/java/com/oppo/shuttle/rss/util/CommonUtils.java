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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

/**
 * CommonUtils include String, System, RetryControl utils
 * @author oppo
 */
public class CommonUtils {
    private static final Logger logger = LoggerFactory.getLogger(CommonUtils.class);

    // System utils
    public static long getUsedMemory() {
        Runtime runtime = Runtime.getRuntime();
        logger.info("Jvm totalMemory: {}, freeMemory: {}", runtime.totalMemory(), runtime.freeMemory());
        return runtime.totalMemory() - runtime.freeMemory();
    }

    // max memory = -Xmx config size
    public static long getJvmConfigMaxMemory() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.maxMemory();
    }

    public static void runGcImmediately() {
        Runtime runtime = Runtime.getRuntime();
        System.gc();
        runtime.runFinalization();
        System.gc();
    }

    public static void safeSleep(long mills) {
        try {
            Thread.sleep(mills);
        } catch (InterruptedException e) {
            logger.error("InterruptedException in safeSleep: ", e);
        }
    }

    // retry utils
    public static <T> T retry(long intervalMillis, long timeoutMills, Supplier<T> callable) {
        long start = System.currentTimeMillis();
        do {
            T result = callable.get();
            if (result != null) {
                return result;
            }

            try {
                Thread.sleep(intervalMillis);
            } catch (InterruptedException e) {
                logger.error("InterruptedException in sleep: ", e);
                break;
            }
        } while (System.currentTimeMillis() - start <= timeoutMills);
        return null;
    }

    public static <T> Collection<T> retryUntilNotEmpty(long intervalMillis, long timeoutMills, Supplier<Collection<T>> callable) {
        long start = System.currentTimeMillis();
        do {
            Collection<T> result = callable.get();
            if (result != null && !result.isEmpty()) {
                return result;
            }
            safeSleep(intervalMillis);
        } while (System.currentTimeMillis() - start <= timeoutMills);
        return Collections.emptyList();
    }

}
