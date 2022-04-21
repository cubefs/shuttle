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

package com.oppo.shuttle.rss.clients;

import java.io.File;

public class ShuffleFileInfo {
  public enum ShuffleFileType {
    SHUFFLE_DATA_TYPE,
    SHUFFLE_DATA_FINAL_TYPE,    // finished dumping data file
    SHUFFLE_INVALID_TYPE
  }

  private File dataFile;
  private ShuffleFileType datFileType;
  private boolean readerInited = false;

  public ShuffleFileInfo() {
  }

  public File getDataFile() {
    return dataFile;
  }

  public void setDataFile(File dataFile) {
    this.dataFile = dataFile;
  }

  public ShuffleFileType getDatFileType() {
    return datFileType;
  }

  public void setDatFileType(ShuffleFileType datFileType) {
    this.datFileType = datFileType;
  }

  public boolean isReaderInited() {
    return readerInited;
  }

  public void setReaderInited(boolean readerInited) {
    this.readerInited = readerInited;
  }
}
