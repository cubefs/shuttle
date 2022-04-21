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

import org.codehaus.jackson.annotate.JsonProperty;

public class Ors2MetricRegisterBean {
    @JsonProperty("ID")
    private String id;

    @JsonProperty("Name")
    private String name = "shuffle_worker";

    @JsonProperty("Address")
    private String address;

    @JsonProperty("Port")
    private int port;

    public static class MetaClass  {
        private String category = "";
        private String dataset = "";
        private String zonecode = "";
        private String metric_path = "/metrics";
        private String group = "ors2";

        public String getCategory() {
            return category;
        }

        public String getDataset() {
            return dataset;
        }

        public String getZonecode() {
            return zonecode;
        }

        public String getMetric_path() {
            return metric_path;
        }

        public String getGroup() {
            return group;
        }
    }

    @JsonProperty("Meta")
    private MetaClass meta = new MetaClass();

    @JsonProperty("EnableTagOverride")
    private boolean enableTagOverride = true;

    public Ors2MetricRegisterBean(String serverKey, String address, int port) {
        this.address = address;
        this.port = port;
        id = "ors2_" + serverKey;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public MetaClass getMeta() {
        return meta;
    }

    public boolean isEnableTagOverride() {
        return enableTagOverride;
    }
}
