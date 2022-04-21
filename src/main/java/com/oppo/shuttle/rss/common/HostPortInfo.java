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

package com.oppo.shuttle.rss.common;

import com.oppo.shuttle.rss.exceptions.Ors2IllegalArgumentException;
import org.apache.parquet.Strings;

import java.util.Objects;

public class HostPortInfo {
    private final String host;
    private final int port;

    public HostPortInfo(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public static HostPortInfo parseFromStr(String str) {
        if (Strings.isNullOrEmpty(str)) {
            return null;
        }

        String[] strArray = str.split(":");
        if (strArray.length == 2) {
            try {
                int port = Integer.parseInt(strArray[1]);
                return new HostPortInfo(strArray[0], port);
            } catch (NumberFormatException e) {
                throw new Ors2IllegalArgumentException("Invalid HostPortInfo string: " + str);
            }
        } else {
            throw new Ors2IllegalArgumentException("Invalid HostPortInfo string: " + str);
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HostPortInfo that = (HostPortInfo) o;
        return port == that.port &&
                Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }
}
