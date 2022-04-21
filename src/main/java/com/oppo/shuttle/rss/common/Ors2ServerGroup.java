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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;

public class Ors2ServerGroup implements java.io.Serializable {

  @JsonProperty("servers")
  public List<Ors2WorkerDetail> getServers() {
    return servers;
  }

  private final List<Ors2WorkerDetail> servers;

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Ors2ServerGroup that = (Ors2ServerGroup) o;
    return Objects.equals(servers, that.servers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(servers);
  }

  @JsonCreator
  public Ors2ServerGroup(@JsonProperty("servers") List<Ors2WorkerDetail> servers) {
    this.servers = servers;
  }

  @Override
  public String toString() {
    return "Ors2ServerGroup{" + StringUtils.join(servers, ',') + '}';
  }
}
