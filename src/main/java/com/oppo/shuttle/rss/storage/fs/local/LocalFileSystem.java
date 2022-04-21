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

package com.oppo.shuttle.rss.storage.fs.local;

import com.oppo.shuttle.rss.common.Ors2FilesystemConf;
import com.oppo.shuttle.rss.storage.fs.FSDataInputStream;
import com.oppo.shuttle.rss.storage.fs.FSDataOutputStream;
import com.oppo.shuttle.rss.storage.fs.FileStatus;
import com.oppo.shuttle.rss.storage.fs.Path;
import com.oppo.shuttle.rss.util.FileUtils;
import com.oppo.shuttle.rss.storage.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.List;

public class LocalFileSystem extends FileSystem {
    private static final Logger logger = LoggerFactory.getLogger(LocalFileSystem.class);

    private static final LocalFileSystem INSTANCE = new LocalFileSystem();

    public LocalFileSystem() {
        this.conf = new Ors2FilesystemConf();
        logger.info("LocalFileSystem load success, defaultFS: /, replication: 1, buffer: 0");
    }

    public boolean isWindows() {
        String osName = System.getProperty("os.name");
        return osName.startsWith("Windows");
    }

    @Override
    public URI getUri() {
        return isWindows() ? URI.create("file:/") : URI.create("file:///");
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        File file = pathToFile(path);
        if (!file.exists()) {
            throw new FileNotFoundException(file.getPath() + " not found");
        }
        return new FileStatus(file.length(), (short) 1, file.isDirectory(), file.lastModified(), path);
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        return open(path);
    }

    @Override
    public FSDataInputStream open(Path path) throws IOException {
        final File file = pathToFile(path);
        return new LocalDataInputStream(file);
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return pathToFile(path).exists();
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        File file = pathToFile(f);
        if (file.isFile()) {
            return file.delete();
        } else if ((!recursive) && file.isDirectory()) {
            File[] containedFiles = file.listFiles();
            if (containedFiles == null) {
                throw new IOException("Directory " + file + " does not exist or an I/O error occurred");
            } else if (containedFiles.length != 0) {
                throw new IOException("Directory " + file + " is not empty");
            }
        }

        return delete(file);
    }

    private boolean delete(final File f) throws IOException {
        if (f.isDirectory()) {
            final File[] files = f.listFiles();
            if (files != null) {
                for (File file : files) {
                    final boolean del = delete(file);
                    if (!del) {
                        return false;
                    }
                }
            }
        } else {
            return f.delete();
        }

        return f.delete();
    }

    @Override
    public boolean mkdirs(Path f) throws IOException {
        File file = pathToFile(f);
        if (file.isDirectory()) {
            return true;
        } else if (file.exists() && !file.isDirectory()) {
            throw new FileAlreadyExistsException(file.getAbsolutePath());
        } else {
            return file.mkdirs();
        }
    }

    @Override
    public FSDataOutputStream create(Path path, boolean overwrite) throws IOException {
        if (!overwrite && exists(path)) {
            throw new FileAlreadyExistsException("File already exists: " + path);
        }

        Path parent = path.getParent();
        if (parent != null && !exists(parent)) {
            mkdirs(parent);
        }

        final File file = pathToFile(path);
        return new LocalDataOutputStream(new FileOutputStream(file));
    }

    @Override
    public FSDataOutputStream append(Path path) throws IOException {
        File file = pathToFile(path);
        if (!file.exists()) {
            return new LocalDataOutputStream(new FileOutputStream(file));
        } else {
            return new LocalDataOutputStream(new FileOutputStream(file, true));
        }
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        File srcFile = pathToFile(src);
        File dstFile = pathToFile(dst);

        if (!srcFile.exists()) {
            throw new FileNotFoundException(src.getPath());
        }
        if (dstFile.exists()) {
            throw new FileAlreadyExistsException(dst.getPath());
        }
        File dstParent = dstFile.getParentFile();
        if (!dstParent.exists()) {
            if (!dstParent.mkdirs()) {
                throw new IOException("Mkdirs failed to create " + dstParent);
            }
        }
        return srcFile.renameTo(dstFile);
    }

    @Override
    public List<FileStatus> listAllFiles(Path dir) throws IOException {
        List<File> files = FileUtils.getFilesRecursive(pathToFile(dir));
        ArrayList<FileStatus> fileStatuses = new ArrayList<>(files.size());

        for (File f : files) {
            fileStatuses.add(getFileStatus(Path.fromFile(f)));
        }
        return fileStatuses;
    }

    @Override
    public List<FileStatus> listStatus(Path path) throws IOException {
        File[] files = path.toFile().listFiles();

        if (files == null) {
            return new ArrayList<>();
        }

        ArrayList<FileStatus> fileStatuses = new ArrayList<>(files.length);
        for (File f : files) {
            fileStatuses.add(getFileStatus(Path.fromFile(f)));
        }

        return fileStatuses;
    }

    @Override
    public boolean isDistributedFS() {
        return false;
    }

    public File pathToFile(Path path) {
        String localPath = path.getPath();
        if (localPath.length() == 0) {
            return new File(".");
        }

        return new File(localPath);
    }

    public static LocalFileSystem getInstance() {
        return INSTANCE;
    }

    @Override
    public String getScheme() {
        return LocalFileSystemFactory.SCHEME;
    }
}
