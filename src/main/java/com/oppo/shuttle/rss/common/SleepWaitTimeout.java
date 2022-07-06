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

import java.util.concurrent.TimeoutException;

public class SleepWaitTimeout {
    private static final Logger logger = LoggerFactory.getLogger(SleepWaitTimeout.class);

    private long start;

    private final long timeoutMS;

    public long sleepTime = 0;

    public SleepWaitTimeout(long timeoutMS) {
        this.timeoutMS = timeoutMS;
        this.start = System.currentTimeMillis();
    }

    public void reset() {
        start = System.currentTimeMillis();
        sleepTime = 0;
    }

    public void sleepAdd(long sleepMS) throws TimeoutException{
        try {
            Thread.sleep(sleepMS);
            sleepTime += sleepMS;
        } catch (Exception e) {
            logger.error("sleep exception", e);
        }

        if (sleepTime > timeoutMS) {
            throw new TimeoutException();
        }
    }

    public void sleepTotal(long sleepMS) throws TimeoutException{
        try {
            Thread.sleep(sleepMS);
        } catch (Exception e) {
            logger.error("sleep exception", e);
        }

        if (System.currentTimeMillis() - start > timeoutMS) {
            throw new TimeoutException();
        }
    }

    public long getDurationMs() {
        return System.currentTimeMillis() - start;
    }
}
