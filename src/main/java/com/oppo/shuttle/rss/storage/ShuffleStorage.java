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

import com.oppo.shuttle.rss.storage.fs.FSDataInputStream;
import com.oppo.shuttle.rss.storage.fs.FileStatus;
import com.oppo.shuttle.rss.storage.fs.FileSystem;

import java.io.File;
import java.util.List;

/***
 * Shuffle storage interface.
 */
public interface ShuffleStorage {

    /***
     * Check whether the file exists.
     * @param path
     * @return
     */
    boolean exists(String path);

    /***
     * List all files under a directory.
     * @param dir
     * @return
     */
    List<String> listAllFiles(String dir);

    List<FileStatus> listStatus(String dir);

    FileStatus getFileStatus(String path);

    /***
     * Create directory and its parents.
     * @param dir
     */
    void createDirectories(String dir);

    /***
     * Delete directory and its children.
     * @param dir
     */
    void deleteDirectory(String dir);

    /***
     * Delete file.
     * @param path
     */
    void deleteFile(String path);
    
    /***
     * Get the size of the file.
     * @param path
     * @return
     */
    long size(String path);

    /***
     * Create a stream for a given file path to write shuffle data.
     * @param path
     * @param compressionCodec
     * @return
     */
    ShuffleOutputStream  createWriterStream(String path, String compressionCodec);

    ShuffleOutputStream createWriterStream(File file, String compressionCodec);

    /***
     * Create a stream for a given file path to read shuffle data.
     * @param path
     * @return
     */
    FSDataInputStream createReaderStream(String path);

    FSDataInputStream createReaderStream(String path, int bufferSize);

    boolean rename(String src, String dst);

    String getRootDir();

    FileSystem getFs();

    String getConfASJson();
}
