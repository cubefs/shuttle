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

package com.oppo.shuttle.rss.storage;

import com.oppo.shuttle.rss.common.Ors2FilesystemConf;
import com.oppo.shuttle.rss.exceptions.Ors2Exception;
import com.oppo.shuttle.rss.storage.fs.FSDataInputStream;
import com.oppo.shuttle.rss.storage.fs.FileStatus;
import com.oppo.shuttle.rss.storage.fs.FileSystem;
import com.oppo.shuttle.rss.storage.fs.Path;
import com.oppo.shuttle.rss.storage.fs.cfs.CfsFileSystemFactory;
import com.oppo.shuttle.rss.storage.fs.dfs.DfsSystemFactory;
import com.oppo.shuttle.rss.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/***
 * FileSystem wrapper
 */
public class ShuffleFileStorage implements ShuffleStorage {
    private static final Logger logger = LoggerFactory.getLogger(ShuffleFileStorage.class);

    private FileSystem fs;

    private String rootDir;

    public ShuffleFileStorage() {
    }

    public ShuffleFileStorage(String uri) {
        try {
            Path path = Path.of(uri);
            fs = FileSystem.get(path.toUri());

            this.rootDir = path.getPath();
        } catch (IOException e) {
            throw new Ors2Exception(e);
        }
    }

    public ShuffleFileStorage(String uri, Ors2FilesystemConf conf) {
        try {
            Path path = Path.of(uri);
            fs = FileSystem.get(path.toUri(), conf);
            this.rootDir = path.getPath();
        } catch (IOException e) {
            throw new Ors2Exception(e);
        }
    }

    @Override
    public boolean exists(String path) {
        try {
            return fs.exists(Path.of(path));
        } catch (IOException e) {
            throw new Ors2Exception(e);
        }
    }

    @Override
    public List<String> listAllFiles(String dir) {
        try {
            return fs.listAllFiles(Path.of(dir))
                    .stream()
                    .map(x -> x.getPath().getPath())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new Ors2Exception("Failed to list directory: " + dir, e);
        }
    }

    @Override
    public List<FileStatus> listStatus(String dir) {
        try {
            return fs.listStatus(Path.of(dir));
        } catch (IOException e) {
            throw new Ors2Exception("Failed to list status directory: " + dir, e);
        }
    }

    @Override
    public FileStatus getFileStatus(String path) {
        try {
            return fs.getFileStatus(Path.of(path));
        } catch (IOException e) {
            throw new Ors2Exception("Failed to get status: " + path, e);
        }
    }

    @Override
    public void createDirectories(String dir) {
        try {
            fs.mkdirs(Path.of(dir));
        } catch (Throwable e) {
            logger.error("Create directories error: ", e);
            throw new Ors2Exception("Failed to create directories: " + dir, e);
        }
    }
    
    @Override
    public void deleteDirectory(String dir) {
        try {
            fs.delete(Path.of(dir), true);
        } catch (Throwable e) {
            throw new Ors2Exception("Failed to delete directory: " + dir, e);
        }
    }

    @Override
    public void deleteFile(String path) {
        try {
            fs.delete(Path.of(path), false);
        } catch (Throwable e) {
            throw new Ors2Exception("Failed to delete file: " + path, e);
        }
    }
    
    @Override
    public long size(String path) {
        try {
            return fs.getFileStatus(Path.of(path)).getLen();
        } catch (Throwable e) {
            logger.warn("dfs get size fail", e);
            return 0;
        }
    }

    @Override
    public ShuffleOutputStream createWriterStream(String path, String compressionCodec) {
        // TODO remove compressionCodec from storage API
        try {
            return new ShuffleFileOutputStream(path, fs.append(Path.of(path)));
        }catch (Throwable e) {
            throw new Ors2Exception("Failed to create file " + path, e);
        }
    }

    @Override
    public ShuffleOutputStream createWriterStream(File file, String compressionCodec) {
        return createWriterStream(file.getPath(), compressionCodec);
    }

    @Override
    public FSDataInputStream createReaderStream(String path) {
       return createReaderStream(path, 65536);
    }

    @Override
    public FSDataInputStream createReaderStream(String path, int bufferSize) {
        try {
            return fs.open(Path.of(path), bufferSize);
        } catch (Throwable e) {
            throw new Ors2Exception("Failed to open file: " + path, e);
        }
    }

    @Override
    public boolean rename(String src, String dst) {
        try {
            return fs.rename(Path.of(src), Path.of(dst));
        } catch (Throwable e) {
            throw new Ors2Exception("Failed to rename" , e);
        }
    }

    @Override
    public String toString() {
        return "ShuffleFileStorage{}";
    }

    @Override
    public String getRootDir() {
        return rootDir;
    }

    @Override
    public FileSystem getFs() {
        return fs;
    }

    @Override
    public String getConfASJson() {
        return JsonUtils.objToJson(fs.getConf().getProps());
    }

    public String getScheme() {
        return fs.getScheme();
    }

    public boolean isCfs() {
        return getScheme().equals(CfsFileSystemFactory.SCHEME);
    }

    public boolean isHdfs() {
        return  getScheme().equals(DfsSystemFactory.SCHEME);
    }

    public void deleteShuffleDataDir(String appId, String shuffleDir) {
        if (isCfs()) {
            String distPath = "/ors2-discard/" + appId;
            rename(shuffleDir, distPath);
            logger.info("App {} shuffle Path {} renamed to {} successfully",
                    appId, getAbsolutePath(shuffleDir), getAbsolutePath(distPath));
        } else {
            deleteDirectory(shuffleDir);
            logger.info("App {} shuffle Path {} deleted successfully", appId, getAbsolutePath(shuffleDir));
        }
    }

    public String getAbsolutePath(String path) {
        return getScheme() + "://" + path;
    }
}
