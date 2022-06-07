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

import com.oppo.shuttle.rss.common.Ors2FilesystemConf;
import com.oppo.shuttle.rss.storage.fs.FileSystemFactory;
import com.oppo.shuttle.rss.storage.fs.alluxio.AlluxioFileSystemFactory;
import com.oppo.shuttle.rss.storage.fs.cfs.CfsFileSystemFactory;
import com.oppo.shuttle.rss.storage.fs.dfs.DfsSystemFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Configuration file loading tool class
 */
public class ConfUtil {
    private static final Logger logger = LoggerFactory.getLogger(ConfUtil.class);

    public static String SPARK_HADOOP_CONF_FILE = "__spark_hadoop_conf__.xml";

    public static final String SPARK_CFS_CONF_FILE = "cfs-site.xml";

    public static final String SPARK_ALLUXIO_CONF_FILE = "alluxio-site.properties";

    public static final String ORS2_CONF_DIR_ENV = "ORS2_CONF_DIR";

    public static final String ORS2_CONF_DIR_PROP = "ors2.conf.dir";

    public static final String BLACK_LIST_CONF = "black-list.conf";

    public static String getOrs2ConfDir() {
        String confDir = System.getenv(ORS2_CONF_DIR_ENV);
        if (confDir == null) {
            confDir = System.getProperty(ORS2_CONF_DIR_PROP);
        }
        return confDir;
    }

    public static Ors2FilesystemConf getDefaultFileSystemConf(FileSystemFactory factory) {
        List<String> fileList = new ArrayList<>(1);
        List<String> classPathList = new ArrayList<>(1);

        switch (factory.getScheme()) {
            case DfsSystemFactory.SCHEME:
                classPathList.add(SPARK_HADOOP_CONF_FILE);
                fileList.add("core-site.xml");
                fileList.add("hdfs-site.xml");
                break;

            case AlluxioFileSystemFactory.SCHEME:
                classPathList.add(SPARK_ALLUXIO_CONF_FILE);
                fileList.add(SPARK_ALLUXIO_CONF_FILE);
                break;

            case CfsFileSystemFactory.SCHEME:
                classPathList.add(SPARK_CFS_CONF_FILE);
                fileList.add(SPARK_CFS_CONF_FILE);
                break;
            default:
                break;
        }

        return getDefaultFileSystemConf(fileList, classPathList);
    }

    public static Ors2FilesystemConf getDefaultFileSystemConf(List<String> fileList, List<String> classPathList) {
        Ors2FilesystemConf filesystemConf = new Ors2FilesystemConf();

        // for shuffle worker
        String confDir = getOrs2ConfDir();
        if (!StringUtils.isEmpty(confDir) && new File(confDir).exists()) {
            for (String path : fileList) {
                filesystemConf.addResource(Paths.get(confDir, path).toString(), Ors2FilesystemConf.TARGET_FILE);
            }
        }

        // hadoop default and spark overlayed config
        classPathList.forEach(file -> {
            filesystemConf.addResource(file, Ors2FilesystemConf.TARGET_CLASSPATH);
        });

        return filesystemConf;
    }

    public static List<String> getWorkerBlackList() {
        String confDir = getOrs2ConfDir();
        ArrayList<String> blackList = new ArrayList<>();
        if (!StringUtils.isEmpty(confDir) && new File(confDir).exists()) {
            File blackListFile = Paths.get(confDir, BLACK_LIST_CONF).toFile();

            if (blackListFile.exists()) {
                try (FileReader fileReader = new FileReader(blackListFile);
                     BufferedReader bufferedReader = new BufferedReader(fileReader)) {
                    String line = bufferedReader.readLine();
                    while (!StringUtils.isEmpty(line)) {
                        if ((!line.contains(":") && !line.contains("_"))
                                || line.contains("#")) {
                            continue;
                        }
                        line = line.replace(":", "_");
                        blackList.add(line.trim());
                        line = bufferedReader.readLine();
                    }
                } catch (IOException e) {
                    logger.error("Read black list conf error: ", e);
                }
            }
        }
        return blackList;
    }
}
