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

import java.io.Serializable;

public class Pair<K, V> implements Serializable {
  private K key;
  private V value;

  public K getKey() {
    return this.key;
  }

  public V getValue() {
    return this.value;
  }

  public void setKey(K k) {key = k;}

  public void setValue(V v) {value = v;}

  public Pair() {
  }

  public Pair(K var1, V var2) {
    this.key = var1;
    this.value = var2;
  }

  @Override
  public String toString() {
    return this.key + "=" + this.value;
  }

  @Override
  public int hashCode() {
    byte var1 = 7;
    int var2 = 31 * var1 + (this.key != null ? this.key.hashCode() : 0);
    var2 = 31 * var2 + (this.value != null ? this.value.hashCode() : 0);
    return var2;
  }

  @Override
  public boolean equals(Object var1) {
    if (this == var1) {
      return true;
    } else if (!(var1 instanceof Pair)) {
      return false;
    } else {
      Pair var2 = (Pair)var1;
      if (this.key != null) {
        if (!this.key.equals(var2.key)) {
          return false;
        }
      } else if (var2.key != null) {
        return false;
      }

      if (this.value != null) {
        if (!this.value.equals(var2.value)) {
          return false;
        }
      } else if (var2.value != null) {
        return false;
      }

      return true;
    }
  }
}
