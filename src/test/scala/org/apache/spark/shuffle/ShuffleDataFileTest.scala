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

package org.apache.spark.shuffle

import com.oppo.shuttle.rss.common.{Constants, PartitionShuffleId, StageShuffleId}
import com.oppo.shuttle.rss.execution.{ShuffleIndex, ShufflePartitionUnsafeWriter}
import com.oppo.shuttle.rss.storage.ShuffleFileStorage
import org.testng.annotations.Test

import java.io.{EOFException, File}

class ShuffleDataFileTest {
  val storage = new ShuffleFileStorage("file:///")

  val file =  new File(s"rss-data/shuffle-data-file${Constants.SHUFFLE_DATA_FILE_POSTFIX}")
  if (file.exists()) {
    file.delete()
  }

  val appShuffleId = new StageShuffleId("test", "0", 0, 0)

  val shufflePartitionId = new PartitionShuffleId(appShuffleId, 0)

  @Test
  def testRead(): Unit = {
    val writer = new ShufflePartitionUnsafeWriter(shufflePartitionId,
      file.getPath.replace(Constants.SHUFFLE_DATA_FILE_POSTFIX, ""), 0, storage)

    writer.writeData("123".getBytes, 0, 0, 0)
    writer.writeData("456".getBytes, 0, 0, 1)
    writer.writeData("789".getBytes, 0, 0, 2)

    writer.close()

    val reader = storage.createReaderStream(file.getPath)
    file.deleteOnExit()

    var num: Int = 0
    var isEnd: Boolean = false
    while (!isEnd) {
      try {
        val index = ShuffleIndex.deserializeFromStream(reader)

        assert(index.getMapId == 0)
        assert(index.getSeqId == num)
        assert(index.getAttemptId == 0)
        val str = new String(reader.readFully(index.getLength.toInt))

        num match {
          case 0 => assert(str == "123")
          case 1 => assert(str == "456")
          case 2 => assert(str == "789")
        }

        num += 1
      } catch  {
        case e: EOFException =>  isEnd = true
      }
    }

    reader.close()
  }
}
