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

import com.oppo.shuttle.rss.exceptions.Ors2Exception;
import com.oppo.shuttle.rss.messages.ShuffleMessage;
import com.oppo.shuttle.rss.util.ByteUtil;
import com.oppo.shuttle.rss.util.JsonUtils;
import com.fasterxml.jackson.databind.JsonNode;
import io.netty.buffer.ByteBuf;

import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * @author oppo
 */
public class Ors2WorkerDetail implements java.io.Serializable {

  private final String host;
  private final int dataPort;
  private final int buildConnPort;

  public Ors2WorkerDetail(String host, int dataPort, int buildConnPort) {
    this.host = host;
    this.dataPort = dataPort;
    this.buildConnPort = buildConnPort;
  }

  /**
   * hostIpPort include hostIp:dataPort:buildConnPort
   * Be careful, this construct method must use the format [hostIp:dataPort:buildConnPort]
   * @param hostIpPort
   */
  public Ors2WorkerDetail(String hostIpPort) {
    String[] params = hostIpPort.split(":");
    host = params[0];
    dataPort = Integer.parseInt(params[1]);
    buildConnPort = Integer.parseInt(params[2]);
  }

  public void serialize(ByteBuf buf) {
    ByteUtil.writeString(buf, host);
    buf.writeInt(dataPort);
    buf.writeInt(buildConnPort);
  }

  public static Ors2WorkerDetail deserialize(ByteBuf buf) {
    String host = ByteUtil.readString(buf);
    int dataPort = buf.readInt();
    int buildConnPort = buf.readInt();
    return new Ors2WorkerDetail(host, dataPort, buildConnPort);
  }

  public String getHost() {
    return host;
  }

  public int getDataPort() {
    return dataPort;
  }

  public int getBuildConnPort() {
    return buildConnPort;
  }

  public String getServerId(){
    return host + "_" + dataPort;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (obj instanceof Ors2WorkerDetail) {
      Ors2WorkerDetail o = (Ors2WorkerDetail) obj;
      if (this.host.equals(o.host) &&
              this.buildConnPort == o.buildConnPort &&
              this.dataPort == o.dataPort) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, dataPort, buildConnPort);
  }

  @Override
  public String toString() {
    return createSimpleString(this.host, this.dataPort, this.buildConnPort);
  }

  public InetSocketAddress buildAddress() {
    return InetSocketAddress.createUnresolved(host, buildConnPort);
  }

  public InetSocketAddress dataAddress() {
    return InetSocketAddress.createUnresolved(host, dataPort);
  }

  public static String createSimpleString(String host, int dataPort, int buildConnPort) {
    StringBuilder sb = new StringBuilder(host).append(":").append(dataPort).append(":").append(buildConnPort);
    return sb.toString();
  }

  public static Ors2WorkerDetail fromSimpleString(String str) {
    String[] split = str.split(":");
    if (split.length != Ors2WorkerDetail.class.getDeclaredFields().length) {
      throw new Ors2Exception("Illegal worker detail string: " + str);
    }
    return new Ors2WorkerDetail(split[0], Integer.parseInt(split[1]), Integer.parseInt(split[2]));
  }

  public static Ors2WorkerDetail fromJsonString(String str) {
    JsonNode node = JsonUtils.jsonToObj(str);
    return new Ors2WorkerDetail(
            node.get("host").asText(),
            node.get("dataPort").asInt(),
            node.get("buildConnPort").asInt()
    );

  }

  public static Ors2WorkerDetail convertFromProto(ShuffleMessage.ServerDetail serverDetail) {
    return new Ors2WorkerDetail(serverDetail.getHost(), serverDetail.getDataPort(), serverDetail.getBuildConnPort());
  }

  public static ShuffleMessage.ServerDetail convertToProto(Ors2WorkerDetail ors2WorkerDetail) {
    return ShuffleMessage.ServerDetail.newBuilder()
            .setHost(ors2WorkerDetail.getHost())
            .setDataPort(ors2WorkerDetail.getDataPort())
            .setBuildConnPort(ors2WorkerDetail.getBuildConnPort())
            .build();
  }


}
