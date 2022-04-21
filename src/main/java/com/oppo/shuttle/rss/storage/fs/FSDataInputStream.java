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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public abstract class FSDataInputStream extends InputStream {

    public abstract void seek(long desired) throws IOException;

    public abstract long getPos() throws IOException;

    public abstract int read(ByteBuffer buffer) throws IOException;

    public final void readFully(byte[] bytes, int off, int len) throws IOException {
        if (len < 0) {
            throw new IndexOutOfBoundsException();
        }

        int n = 0;
        while (n < len) {
            int count = read(bytes, off + n, len - n);
            if (count < 0)
                throw new EOFException();
            n += count;
        }
    }

    public final void readFully(byte[] bytes) throws IOException{
        readFully(bytes, 0, bytes.length);
    }

    public final byte[] readFully(int len) throws IOException {
        byte[] bytes = new byte[len];
        readFully(bytes, 0, bytes.length);
        return bytes;
    }
}