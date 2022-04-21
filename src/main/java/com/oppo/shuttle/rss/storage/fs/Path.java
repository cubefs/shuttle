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

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.Serializable;
import java.net.URI;

public class Path implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String SEPARATOR = "/";

    private org.apache.hadoop.fs.Path path;

    public Path(URI uri) {
        path = new org.apache.hadoop.fs.Path(uri);
    }

    public Path(String pathString) {
        path = new org.apache.hadoop.fs.Path(pathString);
    }

    public URI toUri() {
        return path.toUri();
    }

    public String getName() {
        return path.getName();
    }

    public String getPath() {
        return path.toUri().getPath();
    }

    public File toFile() {
        return new File(getPath());
    }

    public Path getParent() {
        return fromDfs(path.getParent());
    }

    public static Path of(String...arg) {
        String path = StringUtils.join(arg, SEPARATOR);
        return new Path(path);
    }

    public static Path fromFile(File file) {
        return new Path(file.toURI());
    }

    public static Path fromDfs(org.apache.hadoop.fs.Path path) {
        return new Path(path.toUri());
    }

    @Override
    public String toString() {
        return path.toUri().toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Path)) {
            return false;
        }
        Path that = (Path) o;
        return this.toUri().equals(that.toUri());
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }

}


