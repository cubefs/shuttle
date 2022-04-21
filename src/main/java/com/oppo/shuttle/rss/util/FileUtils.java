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

package com.oppo.shuttle.rss.util;

import com.oppo.shuttle.rss.common.Constants;
import com.oppo.shuttle.rss.exceptions.Ors2FileException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public class FileUtils {

    public static void listFiles(List<File> result, File dir) {
        if (dir != null) {
            File[] files = dir.listFiles();
            assert files != null;
            for (File file : files) {
                if (file.isDirectory()) {
                    listFiles(result, file);
                } else if (file.isFile()) {
                    result.add(file);
                }
            }
        }
    }

    public static List<File> getFilesRecursive(File baseDir) {
        List<File> files = new ArrayList<>();
        listFiles(files, baseDir);
        return files;
    }

    public static File getFinalFileUnderSameDir(File file) {
        String filePath = file.getAbsolutePath();
        if (filePath.endsWith(Constants.SHUFFLE_FINAL_FILE_POSTFIX)) {
            return file;
        }
        return new File(filePath + Constants.SHUFFLE_FINAL_FILE_POSTFIX);
    }

    public static File getFinalFile(File file) {
        String parentPath = file.getParentFile().getPath();
        if (parentPath.endsWith("final")) {
            return file;
        }
        StringBuilder finalPathName = new StringBuilder(parentPath)
          .append("/final/")
          .append(file.getName())
          .append(Constants.SHUFFLE_FINAL_FILE_POSTFIX);
        return new File(finalPathName.toString());
    }

    public static long getFileContentSize(String filePath) {
        try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
            return file.length();
        } catch (IOException e) {
            throw new Ors2FileException(String.format("File to get file content size: %s", filePath), e);
        }
    }
}
