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

package com.oppo.shuttle.rss.clients.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class ShuffleWriteHandler {
    private static final Logger logger = LoggerFactory.getLogger(ShuffleWriteHandler.class);

    private final ConcurrentHashMap<String, Request> pendingRequest;

    private final Consumer<Throwable> failureConsumer;

    private final boolean resend;

    public ShuffleWriteHandler(Consumer<Throwable> failureConsumer, boolean resend) {
        this.pendingRequest = new ConcurrentHashMap<>();
        this.failureConsumer = failureConsumer;
        this.resend = resend;
    }

    public Request getRequest(String id) {
        return pendingRequest.get(id);
    }

    public Request removeRequest(String id) {
        return pendingRequest.remove(id);
    }

    public void addRequest(Request request) {
        pendingRequest.put(request.id(), request);
    }

    public int numPendingRequests() {
        return pendingRequest.size();
    }

    public void handleFailure(Throwable cause) {
        LinkedList<Request> requests = new LinkedList<>(pendingRequest.values());
        if (requests.size() == 0) {
            logger.warn("The server is abnormal, but there are no outstanding requests");
            return;
        }
        pendingRequest.clear();

        if (resend) {
            logger.error("The server is abnormal, resend outstanding requests: {}", requests.size(), cause);
            requests.forEach(request -> request.onError(cause));
        } else {
            logger.error("The server is abnormal, not resend outstanding requests: {}", requests.size(), cause);
            failureConsumer.accept(cause);
        }
    }
}
