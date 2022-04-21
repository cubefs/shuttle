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

import org.testng.annotations.Test;

import java.util.Base64;

/**
 * Test for ShuffleInfo class serialize&deserialize
 */
public class ShuffleInfoTest {

    @Test
    public void serializeToString() throws Exception {
        ShuffleInfo.MapShuffleInfo.Builder builder = ShuffleInfo.MapShuffleInfo.newBuilder();
        builder.setMapId(1);
        builder.setAttemptId(1);
        builder.setShuffleWorkerGroupNum(3);
        String st = Base64.getEncoder().encodeToString(builder.build().toByteArray());
        byte[] bytes = Base64.getDecoder().decode(st);
        ShuffleInfo.MapShuffleInfo mapShuffleInfo = ShuffleInfo.MapShuffleInfo.parseFrom(bytes);
        String deStr = Base64.getEncoder().encodeToString(mapShuffleInfo.toByteArray());
        assert st.equals(deStr) : "ShuffleInfo protobuf serialize&deserialize test failed";
    }
}
