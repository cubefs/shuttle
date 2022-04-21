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

package com.oppo.shuttle.rss.storage.fs.dfs;

import com.oppo.shuttle.rss.common.Ors2FilesystemConf;
import com.oppo.shuttle.rss.storage.fs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.List;

/**
 * hdfs file system
 */
public class DfsFileSystem extends FileSystem {
    private static final Logger logger = LoggerFactory.getLogger(DfsFileSystem.class);

    protected org.apache.hadoop.fs.FileSystem fs;

    public DfsFileSystem(org.apache.hadoop.fs.FileSystem dfsFileSystem) {
        fs = dfsFileSystem;
    }

    public DfsFileSystem(URI uri, Ors2FilesystemConf ors2Conf) {
        try {
            this.conf = ors2Conf;
            Configuration hadoopConf = ors2Conf.convertToHadoopConf();
            if (hadoopConf.get("fs.hdfs.impl") == null) {
                hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            }
            fs = createInternal(uri, hadoopConf);

            logger.info("DfsFileSystem(hadoop) load success, uri: {}, defaultFS: {}, replication: {}, buffer: {}" ,
                    uri,
                    hadoopConf.get("fs.defaultFS"),
                    hadoopConf.get("dfs.replication"),
                    hadoopConf.get("io.file.buffer.size"));
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public DfsFileSystem(){
        // pass
    }


    protected static org.apache.hadoop.fs.FileSystem createInternal(URI uri, Configuration conf) throws IOException {
        return org.apache.hadoop.fs.FileSystem.newInstance(uri, conf);
    }

    public org.apache.hadoop.fs.FileSystem getDfsFileSystem() {
        return this.fs;
    }

    @Override
    public URI getUri() {
        return fs.getUri();
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        org.apache.hadoop.fs.FileStatus fileStatus = fs.getFileStatus(toDfsPath(path));
        return new FileStatus(fileStatus.getLen(), fileStatus.getReplication(),
                fileStatus.isDirectory(), fileStatus.getModificationTime(), path);
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        final org.apache.hadoop.fs.FSDataInputStream inputStream = this.fs.open(toDfsPath(path), bufferSize);
        return new DfsDataInputStream(inputStream);
    }

    @Override
    public FSDataOutputStream append(Path path) throws IOException {
        org.apache.hadoop.fs.Path dfsPath = toDfsPath(path);
        if (!fs.exists(dfsPath)) {
            return new DfsDataOutputStream(fs.create(dfsPath, false));
        } else {
            return new DfsDataOutputStream(fs.append(dfsPath));
        }
    }

    @Override
    public FSDataInputStream open(Path path) throws IOException {
        final org.apache.hadoop.fs.FSDataInputStream inputStream = this.fs.open(toDfsPath(path));
        return new DfsDataInputStream(inputStream);
    }

    @Override
    public FSDataOutputStream create(Path path, boolean overwrite) throws IOException {
        org.apache.hadoop.fs.Path dfsPath = toDfsPath(path);
        if (fs.exists(dfsPath) && !overwrite) {
            throw new FileAlreadyExistsException("File already exists: " + path);
        }
        org.apache.hadoop.fs.FSDataOutputStream outputStream = fs.create(dfsPath, overwrite);
        return new DfsDataOutputStream(outputStream);
    }


    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        return fs.delete(toDfsPath(path), recursive);
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return fs.exists(toDfsPath(path));
    }


    @Override
    public boolean mkdirs(Path path) throws IOException {
        return fs.mkdirs(toDfsPath(path));
    }


    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        org.apache.hadoop.fs.Path srcPath = toDfsPath(src);
        org.apache.hadoop.fs.Path dstPath = toDfsPath(dst);
        if (!fs.exists(srcPath)) {
            throw new FileNotFoundException(src.getPath());
        }
        if (fs.exists(dstPath)) {
            throw new FileAlreadyExistsException(dst.getPath());
        }

        org.apache.hadoop.fs.Path pathParent = dstPath.getParent();
        if (!fs.exists(pathParent)) {
            if (!fs.mkdirs(pathParent)) {
                throw new IOException(pathParent + " create fail");
            }
        }

        return fs.rename(srcPath, dstPath);
    }

    @Override
    public List<FileStatus> listAllFiles(Path dir) throws IOException {
        ArrayList<FileStatus> list = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(toDfsPath(dir), true);
        while (iterator.hasNext()) {
            LocatedFileStatus next = iterator.next();
            if (next.isFile()) {
                FileStatus status = new FileStatus(next.getLen(), next.getReplication(),
                        next.isDirectory(), next.getModificationTime(), Path.fromDfs(next.getPath()));
                list.add(status);
            }
        }

        return list;
    }

    @Override
    public List<FileStatus> listStatus(Path dir) throws IOException {
        org.apache.hadoop.fs.FileStatus[] fileStatuses = fs.listStatus(toDfsPath(dir));
        ArrayList<FileStatus> list = new ArrayList<>(fileStatuses.length);

        for(org.apache.hadoop.fs.FileStatus elm : fileStatuses) {
            FileStatus fileStatus = new FileStatus(elm.getLen(), elm.getReplication(),
                    elm.isDirectory(), elm.getModificationTime(), Path.fromDfs(elm.getPath()));
            list.add(fileStatus);
        }

        return list;
    }

    @Override
    public boolean isDistributedFS() {
        return true;
    }

    public static org.apache.hadoop.fs.Path toDfsPath(Path path) {
        return new org.apache.hadoop.fs.Path(path.toUri());
    }

    @Override
    public String getScheme() {
        return DfsSystemFactory.SCHEME;
    }
}
