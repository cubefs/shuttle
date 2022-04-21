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

import com.oppo.shuttle.rss.util.JsonUtils;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Ors2FilesystemConf implements Serializable {
    private final Map<String, String> props = new HashMap<>();

    public Ors2FilesystemConf() {

    }

    public Ors2FilesystemConf(Configuration configuration) {
        configuration.forEach(elm -> {
            props.put(elm.getKey(), elm.getValue());
        });
    }

    public Configuration convertToHadoopConf(boolean loadDefault) {
        Configuration conf = new Configuration(loadDefault);
        props.forEach(conf::set);
        return conf;
    }

    public Configuration convertToHadoopConf() {
        return convertToHadoopConf(false);
    }

    public static Ors2FilesystemConf parseJsonString(String stringConf) {
        if (StringUtils.isEmpty(stringConf)) {
            return null;
        }

        JsonNode node = JsonUtils.jsonToObj(stringConf);
        Ors2FilesystemConf conf = new Ors2FilesystemConf();
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();

        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> next = fields.next();
            conf.set(next.getKey(), next.getValue().asText(null));
        }

        return conf;
    }


    public void set(String key, String value) {
        props.put(key, value);
    }

    public Map<String, String> getProps() {
        return props;
    }
}
