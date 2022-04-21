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

package com.oppo.shuttle.rss.messages;

public enum ShuffleWorkerStorageType {

    NO_STORAGE(201),
    HDD(202),
    SSD(203),
    MEM(204);

    private final int val;

    ShuffleWorkerStorageType(int val) {
        this.val = val;
    }

    public static ShuffleWorkerStorageType valueOf(int val){
        switch (val){
            case 201:
                return ShuffleWorkerStorageType.NO_STORAGE;
            case 202:
                return ShuffleWorkerStorageType.HDD;
            case 203:
                return ShuffleWorkerStorageType.SSD;
            case 204:
                return ShuffleWorkerStorageType.MEM;
            default:
                return null;
        }
    }

}
