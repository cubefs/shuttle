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

package com.oppo.shuttle.rss.decoders;

import com.google.protobuf.InvalidProtocolBufferException;
import com.oppo.shuttle.rss.exceptions.Ors2Exception;
import com.oppo.shuttle.rss.messages.MessageConstants;
import com.oppo.shuttle.rss.messages.ShuffleData;
import com.oppo.shuttle.rss.messages.ShuffleMessage;
import com.oppo.shuttle.rss.util.NetworkUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Decode two type msg mainly, include shuffleData msg and buildConnection msg
 */
public class ShuffleWorkerMessageDecoder extends LengthFieldBasedFrameDecoder {
  private static final Logger logger = LoggerFactory.getLogger(ShuffleWorkerMessageDecoder.class);

  public ShuffleWorkerMessageDecoder() {
    super(134217728, 4, 4);
  }

  @Override
  protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
    ByteBuf buf = (ByteBuf) super.decode(ctx, in);
    if (buf == null) {
      return null;
    }

    try {
      return getMessage(buf, ctx);
    } finally {
      ReferenceCountUtil.release(buf);
    }
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    super.channelActive(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    super.channelInactive(ctx);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    String shuffleConnInfo = NetworkUtils.getConnectInfo(ctx);
    String msg = "Got exception with: " + shuffleConnInfo;
    logger.error(msg, cause);
    ctx.close();
  }

  private Object getMessage(ByteBuf in, ChannelHandlerContext ctx) throws InvalidProtocolBufferException {
    int controlMessageType = in.readInt();
    int totalSize = in.readInt();
    int headerSize = in.readInt();

    byte[] header = new byte[headerSize];
    in.readBytes(header);


    switch (controlMessageType) {
      case MessageConstants.MESSAGE_BuildConnectionRequest: {
        ShuffleMessage.BuildConnectionRequest buildConnectionRequest = ShuffleMessage
                .BuildConnectionRequest
                .parseFrom(header);
        return buildConnectionRequest;
      }
      case MessageConstants.MESSAGE_UploadPackageRequest: {
        ShuffleMessage.UploadPackageRequest uploadPackage = ShuffleMessage.UploadPackageRequest
                .parseFrom(header);
        return ShuffleData.parseFrom(uploadPackage, in);
      }
      default:
        StringBuilder errorInfo = new StringBuilder("Unsupported message type ").append(controlMessageType)
                .append(" from client ").append(NetworkUtils.getConnectInfo(ctx));
        throw new Ors2Exception(errorInfo.toString());
    }
  }
}
