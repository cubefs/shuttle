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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.oppo.shuttle.rss.exceptions.Ors2Exception;

import java.io.IOException;

public class JsonUtils {

    private static ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    }

    public static String objToJson(Object object) {
        if (object == null) {
            return "";
        }

        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException jpe) {
            throw new Ors2Exception(String.format("Failed to parse obj %s, class: %s", object, object.getClass()), jpe);
        }
    }

    public static JsonNode jsonToObj(String content) {
        try {
            return objectMapper.readTree(content);
        } catch (IOException e) {
            throw new Ors2Exception(String.format("Failed to deserialize from json %s", content), e);
        }
    }
}
