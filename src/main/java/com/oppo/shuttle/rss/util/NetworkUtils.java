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

import com.oppo.shuttle.rss.exceptions.Ors2Exception;
import com.oppo.shuttle.rss.exceptions.Ors2NetworkException;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.*;

public class NetworkUtils {
    private static final Logger logger = LoggerFactory.getLogger(NetworkUtils.class);

    public static String getLocalIp() {
        InetAddress address;
        try {
            address = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            logger.warn("Get local ip failed!");
            throw new Ors2NetworkException("Unable to fetch address details for localhost", e);
        }
        return address.getHostAddress();
    }

    /**
     * Http utils
     * @param url
     * @param data
     * @return
     */
    public static String putForString(String url, String data) {
        HttpClient client = new HttpClient();
        PutMethod httpPut = new PutMethod(url);
        try {
            StringRequestEntity httpEntity = new StringRequestEntity(data,"application/json", "UTF-8");
            httpPut.setRequestEntity(httpEntity);
            int code = client.executeMethod(httpPut);
            String response =  httpPut.getResponseBodyAsString();
            if (code != HttpStatus.SC_OK) {
                throw new Ors2Exception("request fail: " + response);
            }
            return response;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Http utils
     * @param url
     * @param data
     * @return
     */
    public static String postForString(String url, String data) {
        HttpClient client = new HttpClient();
        PostMethod httpPost = new PostMethod(url);
        try {
            StringRequestEntity httpEntity = new StringRequestEntity(data,"application/json", "UTF-8");
            httpPost.setRequestEntity(httpEntity);
            int code = client.executeMethod(httpPost);
            String response =  httpPost.getResponseBodyAsString();
            if (code != HttpStatus.SC_OK) {
                throw new Ors2Exception("request fail: " + response);
            }
            return response;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Netty utils
     * @param ctx
     * @return
     */
    public static String getConnectInfo(ChannelHandlerContext ctx) {
        Channel channel = ctx.channel();
        return String.format("{%s - > %s}", channel.remoteAddress(), channel.localAddress());
    }


}
