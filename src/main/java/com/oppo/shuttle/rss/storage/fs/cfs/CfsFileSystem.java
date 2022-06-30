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

package com.oppo.shuttle.rss.storage.fs.cfs;

import com.oppo.shuttle.rss.common.Ors2FilesystemConf;
import com.oppo.shuttle.rss.storage.fs.dfs.DfsFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

/**
 * cfs file system
 */
public class CfsFileSystem extends DfsFileSystem {
    private static final Logger logger = LoggerFactory.getLogger(CfsFileSystem.class);

    public CfsFileSystem(org.apache.hadoop.fs.FileSystem dfsFileSystem) {
        fs = dfsFileSystem;
    }

    public CfsFileSystem(URI uri, Ors2FilesystemConf ors2Conf) {
        try {
            this.conf = ors2Conf;
            Configuration hadoopConf = ors2Conf.convertToHadoopConf();

            if (hadoopConf.get("fs.cfs.impl") == null) {
                hadoopConf.set("fs.cfs.impl", "io.chubaofs.CfsFileSystem");
            }
            fs = createInternal(uri, hadoopConf);

            logger.info("CfsFileSystem load success, fsName {}, uri: {}, master: {}, replication: 1, buffer: {}",
                    hadoopConf.get("fs.cfs.impl"),
                    uri,
                    hadoopConf.get("cfs.master.address"),
                    hadoopConf.get("cfs.min.buffersize"));
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getScheme() {
        return CfsFileSystemFactory.SCHEME;
    }
}
