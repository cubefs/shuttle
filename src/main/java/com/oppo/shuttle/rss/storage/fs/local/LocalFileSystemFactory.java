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
import com.oppo.shuttle.rss.storage.fs.FileSystem;
import com.oppo.shuttle.rss.storage.fs.FileSystemFactory;

import java.io.IOException;
import java.net.URI;

public class LocalFileSystemFactory implements FileSystemFactory {
    public static final String SCHEME = "file";

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public FileSystem create(URI uri, Ors2FilesystemConf conf) throws IOException {
        return LocalFileSystem.getInstance();
    }
}
