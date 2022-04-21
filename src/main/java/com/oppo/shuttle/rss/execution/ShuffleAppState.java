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

package com.oppo.shuttle.rss.execution;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Store app shuffle status
 * @author oppo
 */
public class ShuffleAppState {

    private final String shuffleAppId;
    private final AtomicLong writeBytes = new AtomicLong();
    private final AtomicLong lastUpdateTime = new AtomicLong(System.currentTimeMillis());

    public ShuffleAppState(String shuffleAppId) {
        this.shuffleAppId = shuffleAppId;
    }

    public final void updateAppLiveLastTime() {
        lastUpdateTime.set(System.currentTimeMillis());
    }
    
    public final long getAppLiveLastTime() {
        return lastUpdateTime.get();
    }

    public final long increaseWriteBytes(long delta) {
        return writeBytes.addAndGet(delta);
    }

    public final long getWriteBytes() {
        return lastUpdateTime.get();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ShuffleAppState { appId=").append(shuffleAppId)
                .append(", writeBytes=").append(writeBytes)
                .append(", lastUpdateTime=").append(lastUpdateTime)
                .append(" }");
        return sb.toString();
    }
}
