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

import com.lmax.disruptor.EventFactory;

public class RunnableEvent {
    private Runnable event;

    public Runnable getEvent() {
        return event;
    }

    public void setEvent(Runnable cmd) {
        event = cmd;
    }

    public final static EventFactory<RunnableEvent> RUNNABLE_EVENT_FACTORY = new EventFactory<RunnableEvent>() {
        @Override
        public RunnableEvent newInstance() {
            return new RunnableEvent();
        }
    };
}
