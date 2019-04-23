/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.protocol.http2;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.serialize.ObjectOutput;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.Codec2;
import org.apache.dubbo.remoting.buffer.ChannelBuffer;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.transport.netty4.NettyChannel;
import org.apache.dubbo.remoting.transport.netty4.http2.StreamPayload;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.protocol.dubbo.DubboCodec;

import io.netty.handler.codec.http2.Http2Headers;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class Http2Codec extends DubboCodec implements Codec2 {

    public static final String NAME = "http2";
    private static final Logger logger = LoggerFactory.getLogger(Http2Codec.class);

    @Override
    public void encode(Channel channel, ChannelBuffer buffer, Object message) throws IOException {
        if (channel instanceof NettyChannel) {
            NettyChannel nettyChannel = (NettyChannel) channel;
            if (message instanceof Request) {
                StreamPayload streamPayload = new StreamPayload(message, buffer);
                nettyChannel.setNettyAttribute(StreamPayload.KEY, streamPayload);
            } else if (message instanceof Response) {
                StreamPayload streamPayload = StreamPayload.ID_PAYLOAD_MAP.get(((Response) message).getId());
                streamPayload.message(message).encodedBuffer(buffer);
                nettyChannel.setNettyAttribute(StreamPayload.KEY, streamPayload);
            }
        }
        super.encode(channel, buffer, message);

        if (logger.isDebugEnabled() && message instanceof Response) {
            Response response = (Response) message;
            if (!response.isHeartbeat()) {
                StreamPayload payload = StreamPayload.ID_PAYLOAD_MAP.get(((Response) message).getId());
                logger.debug("complete encode response: " + response
                        + ", streamId:" + payload.streamId()
                        + ", id:" + response.getId()
                        + ", length:" + buffer.readableBytes());
            }
        }
    }

    @Override
    protected void encodeRequestData(Channel channel, ObjectOutput out, Object data, String version) throws IOException {
        super.encodeRequestData(channel, out, data, version);

        RpcInvocation invoker = (RpcInvocation) data;
        URL url = invoker.getInvoker().getUrl();
        String serviceName = invoker.getAttachment(Constants.PATH_KEY);
        String methodName = invoker.getMethodName();
        String authority = invoker.getAttachment(Constants.HTTP2_AUTHORITY_KEY);
        if (null == authority) authority = "";

        if (channel instanceof NettyChannel) {
            NettyChannel nettyChannel = (NettyChannel) channel;

            // prepare header for http2
            StreamPayload streamPayload = nettyChannel.getNettyAttribute(StreamPayload.KEY);
            if (streamPayload == null) {
                logger.error("Not found stream streamPayload for key 'http2.streamPayload', channel:" + channel + " data:" + data);
                return;
            }
            streamPayload.addAttachment(Constants.HTTP2_SERVICE_KEY, serviceName)
                    .addAttachment(Constants.HTTP2_SERVICE_METHOD_KEY, methodName)
                    .addAttachment(Constants.HTTP2_SCHEME_KEY, url.getParameter(Constants.SSL_ENABLE_KEY, false) ? "https" : "http")
                    .addAttachment(Constants.HTTP2_CONTENT_TYPE_KEY, Constants.HTTP2_DUBBO_CONTENT_TYPE)
                    .addAttachment(Constants.HTTP2_AUTHORITY_KEY, authority);
        }
    }

    // Will be invoked when decode body
    @Override
    protected void attachInvocation(Channel channel, Invocation invocation, Request req) {
        if (channel instanceof NettyChannel) {
            NettyChannel nettyChannel = (NettyChannel) channel;
            StreamPayload streamPayload = nettyChannel.getNettyAttribute(StreamPayload.KEY);

            if (streamPayload != null && streamPayload.http2Headers() != null && invocation instanceof RpcInvocation) {
                Http2Headers headers = streamPayload.http2Headers();
                StreamPayload.ID_PAYLOAD_MAP.put(req.getId(), streamPayload);
                RpcInvocation inv = (RpcInvocation) invocation;
                Iterator<Map.Entry<CharSequence, CharSequence>> iterator = streamPayload.http2Headers().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<CharSequence, CharSequence> entry = iterator.next();
                    if (entry.getKey().toString().equals("X-")) {
                        inv.setAttachment(entry.getKey().toString(),
                                entry.getValue() == null ? "" : entry.getValue().toString());
                    }
                }
            }

            StreamPayload check = StreamPayload.ID_PAYLOAD_MAP.get(req.getId());
            if (streamPayload != check) {
                logger.warn("channel payload:" + (streamPayload != null ? streamPayload : null)
                        + ", check payload(stream map):" + (check != null ? check : null)
                        + ", payload.http2Headers() != null:" + (streamPayload.http2Headers() != null)
                        + ", invocation instanceof RpcInvocation:" + (invocation instanceof RpcInvocation));
            }
        }
    }
}