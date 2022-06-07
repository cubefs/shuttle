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

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.Source;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.util.ConfigurationUtils;
import com.oppo.shuttle.rss.common.Ors2FilesystemConf;
import com.oppo.shuttle.rss.storage.fs.FSDataOutputStream;
import com.oppo.shuttle.rss.storage.fs.FileStatus;
import com.oppo.shuttle.rss.storage.fs.FileSystem;
import com.oppo.shuttle.rss.storage.fs.Path;
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
public class AlluxioFileSystem extends FileSystem {
    private static final Logger logger = LoggerFactory.getLogger(AlluxioFileSystem.class);
    protected final alluxio.client.file.FileSystem fs;
    private final URI uri;

    public AlluxioFileSystem(URI uri, Ors2FilesystemConf ors2Conf) {
        try {
            this.conf = ors2Conf;
            this.uri = uri;
            AlluxioProperties properties = ConfigurationUtils.defaults();
            properties.merge(conf.getProps(), Source.RUNTIME);
            fs = alluxio.client.file.FileSystem.Factory.create(new InstancedConfiguration(properties));

            logger.info("AlluxioFileSystem(alluxio) load success, uri: {}, pool size: {}, block size: {}, buffer: {}" ,
                    uri,
                    conf.get("alluxio.user.block.worker.client.pool.max"),
                    conf.get("alluxio.user.block.size.bytes.default"),
                    conf.get("alluxio.user.file.buffer.bytes"));
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public static AlluxioURI toAlluxioURI(Path path) {
        return new AlluxioURI(path.getPath());
    }

    public static AlluxioURI toAlluxioURI(String path) {
        return new AlluxioURI(path);
    }

    @Override
    public URI getUri() {
        return uri;
    }

    public alluxio.client.file.FileSystem getAlluxioFileSystem() {
        return this.fs;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        AlluxioURI uri = toAlluxioURI(path);
        URIStatus fileStatus;
        try {
            fileStatus = fs.getStatus(uri);
            return new FileStatus(
                    fileStatus.getLength(),
                    (short) fileStatus.getReplicationMin(),
                    fileStatus.isFolder(),
                    fileStatus.getLastModificationTimeMs(),
                    Path.of(fileStatus.getPath())
            );
        } catch (FileDoesNotExistException e) {
            throw new FileNotFoundException(e.getMessage());
        } catch (AlluxioException e) {
            throw new IOException(e);
        }
    }

    @Override
    public AlluxioDataInputStream open(Path path, int bufferSize) throws IOException {
        return open(path);
    }

    @Override
    public AlluxioDataInputStream open(Path path) throws IOException {
        AlluxioURI uri = toAlluxioURI(path);
        try {
            FileInStream stream = fs.openFile(uri);
            return new AlluxioDataInputStream(stream);
        } catch (FileDoesNotExistException e) {
            throw new FileNotFoundException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(uri));
        } catch (AlluxioException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean exists(Path path) throws IOException {
        try {
            return fs.exists(toAlluxioURI(path));
        } catch (InvalidPathException e) {
            return false;
        } catch (AlluxioException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        AlluxioURI uri = toAlluxioURI(path);
        DeletePOptions options = DeletePOptions
                .newBuilder()
                .setRecursive(recursive)
                .build();
        try {
            fs.delete(uri, options);
            return true;
        } catch (InvalidPathException | FileDoesNotExistException e) {
            return false;
        } catch (AlluxioException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        AlluxioURI uri = toAlluxioURI(path);

        CreateDirectoryPOptions options = CreateDirectoryPOptions
                .newBuilder()
                .setRecursive(true)
                .setAllowExists(true)
                .build();
        try {
            fs.createDirectory(uri, options);
            return true;
        } catch (AlluxioException e) {
            throw new IOException(e);
        }
    }

    @Override
    public FSDataOutputStream create(Path path, boolean overwrite) throws IOException {
        AlluxioURI uri = toAlluxioURI(path);
        CreateFilePOptions options = CreateFilePOptions.newBuilder()
                .setRecursive(true)
                .build();

        FileOutStream outStream;
        try {
            outStream = fs.createFile(uri, options);
            fs.setAttribute(uri, SetAttributePOptions.newBuilder().setPinned(true).build());
        } catch (AlluxioException e) {
            try {
                if (fs.exists(uri)) {
                    if (!overwrite) {
                        throw new FileAlreadyExistsException("File already exists: " + path);
                    }

                    if (fs.getStatus(uri).isFolder()) {
                        throw new IOException(
                                ExceptionMessage.FILE_CREATE_IS_DIRECTORY.getMessage(uri));
                    }
                    fs.delete(uri);
                }
                outStream = fs.createFile(uri, options);
            } catch (AlluxioException e1) {
                throw new IOException(e1);
            }
        }

        return new AlluxioDataOutputStream(outStream);
    }

    @Override
    public FSDataOutputStream append(Path path) throws IOException {
        AlluxioURI uri = toAlluxioURI(path);
        try {
            if (fs.exists(uri)) {
                throw new IOException("append() to existing Alluxio path is currently not supported: " + uri);
            }
            CreateFilePOptions options = CreateFilePOptions
                    .newBuilder()
                    .setRecursive(true)
                    .build();
            return new AlluxioDataOutputStream(fs.createFile(uri,options));
        } catch (AlluxioException e) {
            throw new IOException(e);
        }
    }

    private void ensureExists(AlluxioURI path) throws IOException {
        try {
            fs.getStatus(path);
        } catch (AlluxioException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        AlluxioURI srcPath = toAlluxioURI(src);
        AlluxioURI dstPath = toAlluxioURI(dst);

        if (!exists(src)) {
            throw new FileNotFoundException(src.getPath());
        }
        if (exists(dst)) {
            throw new FileAlreadyExistsException(dst.getPath());
        }

        Path dstParent = dst.getParent();
        if (!exists(dstParent)) {
            if (!mkdirs(dstParent)) {
                throw new IOException("Mkdirs failed to create " + dstParent);
            }
        }

        try {
            fs.rename(srcPath, dstPath);
        } catch (FileDoesNotExistException e) {
            logger.warn("rename failed: {}", e.toString());
            return false;
        } catch (AlluxioException e) {
            ensureExists(srcPath);
            URIStatus dstStatus;
            try {
                dstStatus = fs.getStatus(dstPath);
            } catch (IOException | AlluxioException e2) {
                logger.warn("rename failed: {}", e.toString());
                return false;
            }
            // If the destination is an existing folder, try to move the src into the folder
            if (dstStatus != null && dstStatus.isFolder()) {
                dstPath = dstPath.joinUnsafe(srcPath.getName());
            } else {
                logger.warn("rename failed: {}", e.toString());
                return false;
            }
            try {
                fs.rename(srcPath, dstPath);
            } catch (IOException | AlluxioException e2) {
                logger.error("Failed to rename {} to {}", src, dst, e2);
                return false;
            }
        } catch (IOException e) {
            logger.error("Failed to rename {} to {}", src, dst, e);
            return false;
        }
        return true;
    }

    private void listFilesRecursive(AlluxioURI uri, ArrayList<FileStatus> list) throws IOException {
        List<URIStatus> statuses;
        try {
            statuses = fs.listStatus(uri);
        } catch (FileDoesNotExistException e) {
            throw new FileNotFoundException(uri.getPath());
        }catch (AlluxioException e) {
            throw new IOException(e);
        }

        for (URIStatus status : statuses) {
            if (status.isFolder()) {
                listFilesRecursive(toAlluxioURI(status.getPath()), list);
            } else {
                list.add(new FileStatus(
                        status.getLength(),
                        (short) status.getReplicationMin(),
                        status.isFolder(),
                        status.getLastModificationTimeMs(),
                        Path.of(status.getPath())
                ));
            }
        }
    }

    @Override
    public List<FileStatus> listAllFiles(Path dir) throws IOException {
        ArrayList<FileStatus> list = new ArrayList<>();
        listFilesRecursive(toAlluxioURI(dir), list);
        return list;
    }

    @Override
    public List<FileStatus> listStatus(Path dir) throws IOException {
        AlluxioURI uri = toAlluxioURI(dir);
        List<URIStatus> statuses;
        try {
            statuses = fs.listStatus(uri);
        } catch (FileDoesNotExistException e) {
            throw new FileNotFoundException(dir.getPath());
        }catch (AlluxioException e) {
            throw new IOException(e);
        }

        List<FileStatus> ret = new ArrayList<>(statuses.size());
        for (URIStatus status : statuses) {
            ret.add(new FileStatus(
                    status.getLength(),
                    (short) status.getReplicationMin(),
                    status.isFolder(),
                    status.getLastModificationTimeMs(),
                    Path.of(status.getPath())
            ));
        }

        return ret;
    }

    @Override
    public boolean isDistributedFS() {
        return true;
    }

    @Override
    public String getScheme() {
        return AlluxioFileSystemFactory.SCHEME;
    }
}
