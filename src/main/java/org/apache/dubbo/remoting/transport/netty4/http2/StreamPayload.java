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
package org.apache.dubbo.remoting.transport.netty4.http2;

import org.apache.dubbo.remoting.buffer.ChannelBuffer;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Stream;
import io.netty.util.AttributeKey;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StreamPayload {

    public static final String HTTP2_KEY = "http2.payload";
    public static final String HTTP2_STREAM_ID_KEY = "stream.id";

    public static final AttributeKey<StreamPayload> KEY = AttributeKey.valueOf(HTTP2_KEY);

    public static final AttributeKey<Integer> STREAM_ID_KEY = AttributeKey.valueOf(HTTP2_STREAM_ID_KEY);

    // request/response id -> payload
    public static final ConcurrentHashMap<Long, StreamPayload> ID_PAYLOAD_MAP = new ConcurrentHashMap<Long, StreamPayload>(1024);

    private int streamId;

    private Http2Stream http2Stream;
    private Http2Headers headers;
    private ByteBuf data;
    private boolean endOfStream;

    private Object message;
    private ChannelBuffer encodedBuffer;

    private Map<String, String> attachments;

    private AbstractHttp2CodecHandler http2CodecHandler;
    private Http2Connection.PropertyKey streamKey;

    private volatile boolean destroy;

    public StreamPayload(Object message, ChannelBuffer encodedBuffer) {
        this.message = message;
        this.encodedBuffer = encodedBuffer;
    }

    public StreamPayload(Http2Stream http2Stream
            , Http2Headers headers
            , AbstractHttp2CodecHandler http2CodecHandler
            , Http2Connection.PropertyKey streamKey) {
        this.http2Stream = http2Stream;
        this.headers = headers;
        this.http2CodecHandler = http2CodecHandler;
        this.streamKey = streamKey;
    }

    public StreamPayload http2Stream(Http2Stream stream) {
        this.http2Stream = stream;
        return this;
    }

    public StreamPayload http2Headers(Http2Headers headers) {
        this.headers = headers;
        return this;
    }

    public StreamPayload data(ByteBuf data) {
        this.data = data;
        return this;
    }

    public Http2Stream http2Stream() {
        return this.http2Stream;
    }

    public Http2Headers http2Headers() {
        return this.headers;
    }

    public Map<String, String> attachments() {
        if (attachments == null) attachments = new HashMap<String, String>();
        return attachments;
    }

    public StreamPayload attachments(Map<String, String> attachments) {
        if (attachments != this.attachments) {
            this.attachments = attachments;
        }
        return this;
    }

    public ByteBuf data() {
        return this.data;
    }

    public StreamPayload endOfStream(boolean endOfStream) {
        this.endOfStream = endOfStream;
        return this;
    }

    public Object message() {
        return message;
    }

    public StreamPayload message(Object message) {
        this.message = message;
        return this;
    }

    public ChannelBuffer encodedBuffer() {
        return encodedBuffer;
    }

    public StreamPayload encodedBuffer(ChannelBuffer encodedBuffer) {
        this.encodedBuffer = encodedBuffer;
        return this;
    }

    public boolean endOfStream() {
        return this.endOfStream;
    }

    public StreamPayload addAttachment(String key, String value) {
        this.attachments().put(key, value);
        return this;
    }

    public String header(String key) {
        return this.attachments().get(key);
    }

    public String header(String key, boolean remove) {
        if (remove) {
            return this.attachments().remove(key);
        }
        return this.attachments().get(key);
    }

    public int streamId() {
        return streamId;
    }

    public StreamPayload streamId(int streamId) {
        this.streamId = streamId;
        return this;
    }

    public void complete() {

        if (destroy) return;

        destroy = true;

        // help for gc
        if (message != null) {
            if (message instanceof Request) {
                ID_PAYLOAD_MAP.remove(((Request) message).getId());
            } else if (message instanceof Response) {
                ID_PAYLOAD_MAP.remove(((Response) message).getId());
            }
        }

        if (streamKey != null && http2Stream != null) {
            http2Stream.removeProperty(streamKey);
        }

        message = null;
        http2CodecHandler = null;
        headers = null;
        http2Stream = null;
        attachments = null;
        encodedBuffer = null;
        streamKey = null;
        data = null;
    }

    public AbstractHttp2CodecHandler http2CodecHandler() {
        return http2CodecHandler;
    }

    public StreamPayload http2CodecHandler(AbstractHttp2CodecHandler http2CodecHandler) {
        this.http2CodecHandler = http2CodecHandler;
        return this;
    }

    @Override
    public String toString() {
        return "Payload{" +
                "streamId:" + streamId +
                ", headers:" + (headers == null ? "" : headers) +
                ", data:" + (data == null ? "" : data) +
                ", message:" + (message == null ? "" : message) +
                ", attachments:" + (attachments == null ? "" : attachments) +
                '}';
    }
}
