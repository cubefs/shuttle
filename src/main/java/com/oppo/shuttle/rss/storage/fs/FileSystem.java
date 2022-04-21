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

package com.oppo.shuttle.rss.storage.fs;

import com.oppo.shuttle.rss.common.Ors2FilesystemConf;
import com.oppo.shuttle.rss.storage.fs.local.LocalFileSystem;
import com.oppo.shuttle.rss.util.ConfUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

abstract public class FileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(FileSystem.class);

    private static final HashMap<String, FileSystem> cache = new HashMap<>();

    private static final HashMap<String, FileSystemFactory> factories = new HashMap<>();

    protected Ors2FilesystemConf conf;


    public synchronized static void init() {
        cache.clear();
        factories.clear();
        ServiceLoader<FileSystemFactory> serviceLoader = ServiceLoader.load(FileSystemFactory.class);
        for (FileSystemFactory factory : serviceLoader) {
            factories.put(factory.getScheme(), factory);
        }
    }


    public abstract String getScheme();

    public abstract URI getUri();

    public abstract FileStatus getFileStatus(Path path) throws IOException;

    public abstract FSDataInputStream open(Path path, int bufferSize) throws IOException;

    public abstract FSDataInputStream open(Path path) throws IOException;

    public abstract boolean exists(Path path) throws IOException;

    public abstract boolean delete(Path path, boolean recursive) throws IOException;

    public abstract boolean mkdirs(Path path) throws IOException;


    public abstract FSDataOutputStream create(Path path, boolean overwrite) throws IOException;

    public abstract FSDataOutputStream append(Path path) throws IOException;

    public abstract boolean rename(Path src, Path dst) throws IOException;

    public abstract List<FileStatus> listAllFiles(Path dir) throws IOException;

    public abstract List<FileStatus> listStatus(Path dir) throws IOException;

    public abstract boolean isDistributedFS();

    public static FileSystem get(String path) throws IOException{
        return get(Path.of(path).toUri());
    }

    public static FileSystem get(URI uri) throws IOException {
        return get(uri, null);
    }

    public synchronized static FileSystem get(URI uri, Ors2FilesystemConf conf) throws IOException{
        final URI fsUri;
        if (uri.getScheme() == null) {
            URI local = LocalFileSystem.getInstance().getUri();
            try {
                fsUri = new URI(local.getScheme(), null, local.getHost(),
                        local.getPort(), local.getPath(), null, null);
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        } else {
            fsUri = uri;
        }

        String key = fsUri.getScheme() + fsUri.getAuthority();
        FileSystem cachedFileSystem = cache.get(key);
        if (cachedFileSystem != null) {
            return cachedFileSystem;
        }

        if (factories.isEmpty()) {
            init();
        }

        FileSystemFactory factory = factories.get(fsUri.getScheme());
        if (factory == null) {
            throw new UnsupportedFileSystemException(uri.getScheme());
        }

        if (conf == null) {
            conf = ConfUtil.getDefaultFileSystemConf(factory);
        }

        FileSystem fs = factory.create(uri, conf);
        cache.put(key, fs);
        return fs;
    }

    public Ors2FilesystemConf getConf() {
       return conf;
    }
}

