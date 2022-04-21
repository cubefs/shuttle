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

package com.oppo.shuttle.rss.storage.fs.alluxio;

import com.oppo.shuttle.rss.common.Ors2FilesystemConf;
import com.oppo.shuttle.rss.storage.fs.FileStatus;
import com.oppo.shuttle.rss.storage.fs.Path;
import com.oppo.shuttle.rss.storage.fs.dfs.DfsFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * hdfs file system
 */
public class AlluxioFileSystem extends DfsFileSystem {
    private static final Logger logger = LoggerFactory.getLogger(AlluxioFileSystem.class);

    public AlluxioFileSystem(org.apache.hadoop.fs.FileSystem dfsFileSystem) {
        fs = dfsFileSystem;
    }

    public AlluxioFileSystem(URI uri, Ors2FilesystemConf ors2Conf) {
        try {
            this.conf = ors2Conf;
            Configuration hadoopConf = ors2Conf.convertToHadoopConf();
            if (hadoopConf.get("fs.alluxio.impl") == null) {
                hadoopConf.set("fs.alluxio.impl", "alluxio.hadoop.FileSystem");
            }
            fs = createInternal(uri, hadoopConf);

            logger.info("AlluxioFileSystem(alluxio) load success, uri: {}, pool size: {}, block size: {}, buffer: {}" ,
                    uri,
                    hadoopConf.get("alluxio.worker.block.master.client.pool.size"),
                    hadoopConf.get("alluxio.user.block.size.bytes.default"),
                    hadoopConf.get("alluxio.user.file.buffer.bytes"));
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private void listFilesRecursive(org.apache.hadoop.fs.Path path, ArrayList<FileStatus> list) throws IOException {
        org.apache.hadoop.fs.FileStatus[] fileStatuses = fs.listStatus(path);
        for (org.apache.hadoop.fs.FileStatus status : fileStatuses) {
            if (status.isDirectory()) {
                listFilesRecursive(status.getPath(), list);
            } else {
                FileStatus fileStatus = new FileStatus(status.getLen(), status.getReplication(),
                        status.isDirectory(), status.getModificationTime(), Path.fromDfs(status.getPath()));
                list.add(fileStatus);
            }
        }
    }

    @Override
    public List<FileStatus> listAllFiles(Path dir) throws IOException {
        ArrayList<FileStatus> list = new ArrayList<>();
        listFilesRecursive(toDfsPath(dir), list);
        return list;
    }

    @Override
    public String getScheme() {
        return AlluxioFileSystemFactory.SCHEME;
    }
}
