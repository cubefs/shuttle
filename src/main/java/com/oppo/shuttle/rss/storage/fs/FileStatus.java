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

public class FileStatus {
    private final long len;

    private final short replication;

    private final boolean _isDir;

    private final Path path;

    private final long modificationTime;

    public FileStatus(long len, short replication, boolean _isDir, long modificationTime, Path path) {
        this.len = len;
        this.replication = replication;
        this._isDir = _isDir;
        this.modificationTime = modificationTime;
        this.path = path;
    }

    public long getLen() {
        return len;
    }

    public short getReplication() {
        return replication;
    }

    public boolean isDir() {
        return _isDir;
    }

    public Path getPath() {
        return path;
    }

    public long getModificationTime() {
        return modificationTime;
    }

    @Override
    public String toString() {
        return "FileStatus{" +
                "len=" + len +
                ", replication=" + replication +
                ", _isDir=" + _isDir +
                ", path=" + path +
                ", modificationTime=" + modificationTime +
                '}';
    }
}
