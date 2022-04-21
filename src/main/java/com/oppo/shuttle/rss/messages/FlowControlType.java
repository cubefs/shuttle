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

package com.oppo.shuttle.rss.messages;

public enum FlowControlType {
  NO_CONTROL(101), MEM_CONTROL(102), BUSY_CONTROL(103), BYPASS_PASS(104);

  private final int value;

  FlowControlType(int val) {
    this.value = val;
  }

  public static FlowControlType valueOf(int val) {
    switch (val) {
      case 101:
        return FlowControlType.NO_CONTROL;
      case 102:
        return FlowControlType.MEM_CONTROL;
      case 103:
        return FlowControlType.BUSY_CONTROL;
      case 104:
        return FlowControlType.BYPASS_PASS;
      default:
        return null;
    }
  }
}

