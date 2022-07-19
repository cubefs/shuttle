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

import com.oppo.shuttle.rss.exceptions.Ors2Exception;
import com.oppo.shuttle.rss.util.JsonUtils;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class Ors2FilesystemConf implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(Ors2FilesystemConf.class);

    public static final String EXT_XML = "xml";

    public static final String EXT_PROPERTIES = "properties";

    public static final String TARGET_FILE = "file";

    public static final String TARGET_CLASSPATH = "classpath";

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

    public String get(String key) {
        return props.get(key);
    }

    public String get(String key, String defaultValue) {
        return props.getOrDefault(key, defaultValue);
    }

    public void addResource(String path, String tpe) {
        File file = new File(path);
        URL url = Utils.getContextOrSparkClassLoader().getResource(path);
        String ext = FilenameUtils.getExtension(path);

        switch (ext) {
            case EXT_XML:
                Configuration configuration = new Configuration(false);

                if (tpe.equals(TARGET_FILE)) {
                    if (file.exists() && file.canRead() && !file.isDirectory()) {
                        configuration.addResource(new org.apache.hadoop.fs.Path(file.getPath()));
                        logger.info("Add xml file config: " + path);
                    }
                } else if (tpe.equals(TARGET_CLASSPATH)) {
                    if (url != null) {
                        configuration.addResource(url);
                        logger.info("Add xml classpath config: " + url);
                    }
                }

                configuration.forEach(elm -> props.put(elm.getKey(), elm.getValue()));
                break;

            case EXT_PROPERTIES:
                Properties properties = new Properties();

                try {
                    if (tpe.equals(TARGET_FILE)) {
                        if (file.exists() && file.canRead() && !file.isDirectory()) {
                            properties.load(new FileInputStream(file));
                            logger.info("Add properties file config: " + path);
                        }
                    } else if (tpe.equals(TARGET_CLASSPATH)) {
                        if (url != null) {
                            properties.load(url.openStream());
                            logger.info("Add properties classpath config: " + url);
                        }
                    }
                } catch (IOException e) {
                    throw new Ors2Exception(e);
                }

                for (String key : properties.stringPropertyNames()) {
                    props.put(key, properties.getProperty(key));
                }

                break;
            default:
                break;
        }
    }
}
