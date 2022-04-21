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

package com.oppo.shuttle.rss.execution;

public class ConnectionValue {

  private long connectionValue = -1L;
  private long assignTimeStamp = -1L;

  public ConnectionValue() {
    connectionValue = 0;
  }

  public ConnectionValue(long value) {
    setConnectionValue(value);
    setAssignTimeStamp(System.currentTimeMillis());
  }

  public void update() {
    connectionValue += 1;
    assignTimeStamp = System.currentTimeMillis();
  }

  public void reset() {
    assignTimeStamp = -1L;
  }

  public long getConnectionValue() {
    return connectionValue;
  }

  public void setConnectionValue(long connectionValue) {
    this.connectionValue = connectionValue;
  }

  public long getAssignTimeStamp() {
    return assignTimeStamp;
  }

  public void setAssignTimeStamp(long assignTimeStamp) {
    this.assignTimeStamp = assignTimeStamp;
  }

  public boolean expired(long currentTime, long timeout) {
    if (assignTimeStamp == -1) {
      return false; // unassigned
    }
    return currentTime - assignTimeStamp > timeout;
  }
}
