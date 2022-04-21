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

import java.util.concurrent.*;

public class ScheduledThreadPoolUtils {

    public static ScheduledThreadPoolExecutor threadPool;

    static {
        threadPool = new ScheduledThreadPoolExecutor(4, new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public static void shutdown() {
        threadPool.shutdown();
    }

    public static void scheduleAtFixedRate(Runnable runnable, long delay, long period){
        if (threadPool.isShutdown()){
            threadPool = new ScheduledThreadPoolExecutor(4, new ThreadPoolExecutor.CallerRunsPolicy());
        }
        threadPool.scheduleAtFixedRate(runnable, delay, period, TimeUnit.MILLISECONDS);
    }

}
