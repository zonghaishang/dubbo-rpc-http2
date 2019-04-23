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

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.Request;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2ConnectionDecoder;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2Stream;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;

import static io.netty.handler.codec.http2.Http2CodecUtil.getEmbeddedHttp2Exception;

public abstract class AbstractHttp2CodecHandler extends Http2ConnectionHandler {

    static final Object NOOP_MESSAGE = new Object();
    private static final long BDP_MEASUREMENT_PING = 1234;
    protected static int DEFAULT_FLOW_CONTROL_WINDOW = 1048576; // 1MiB
    private final Logger logger = LoggerFactory.getLogger(AbstractHttp2CodecHandler.class);
    protected URL url;
    protected Http2Connection.PropertyKey streamKey;
    private int initialConnectionWindow;
    private ChannelHandlerContext ctx;
    private boolean autoTuneFlowControlOn = false;

    public AbstractHttp2CodecHandler(URL url,
                                     Http2ConnectionDecoder decoder,
                                     Http2ConnectionEncoder encoder,
                                     Http2Settings initialSettings) {
        super(decoder, encoder, initialSettings);
        this.url = url;
        this.initialConnectionWindow = initialSettings.initialWindowSize() == null ? -1 :
                initialSettings.initialWindowSize();
        this.streamKey = encoder.connection().newKey();
    }

    protected Http2Headers prepareHeaders(Request request, StreamPayload payload) {
        Http2Headers headers = new DefaultHttp2Headers();

        String contentType = payload.header(Constants.HTTP2_CONTENT_TYPE_KEY, true);
        headers.add(Constants.HTTP2_CONTENT_TYPE_KEY, contentType == null ? Constants.HTTP2_DUBBO_CONTENT_TYPE : contentType);

        /** http2 content-length */
        headers.setInt(Constants.HTTP2_CONTENT_LENGTH_KEY, payload.encodedBuffer().readableBytes());

        if (!request.isEvent()) {
            headers.method(Constants.POST_KEY)
                    .path("/" + payload.header(Constants.HTTP2_SERVICE_KEY, true) +
                            "/" + payload.header(Constants.HTTP2_SERVICE_METHOD_KEY, true))
                    .scheme(payload.header(Constants.HTTP2_SCHEME_KEY, true));

            if (payload.attachments() != null) {
                for (Map.Entry<String, String> attachment : payload.attachments().entrySet()) {
                    headers.add(attachment.getKey(), attachment.getValue());
                }
            }
        } else {
            headers.method(Constants.GET_KEY)
                    .path(Constants.EVENT_PATH);
        }
        return headers;
    }

    protected void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, boolean endStream) throws Http2Exception {
        try {

            Channel channel = ctx.channel();
            if (headers.status() != null && Constants.HTTP2_OK_STATUS.equals(headers.status().toString())) {
                /**
                 * We don't care about the responder's header,
                 * We care more about the last header and response data.
                 */
                return;
            }
            /** This should not be triggered unless the parameter callback function is enabled. */
            if (endStream) {
                StreamPayload payload = payloadOfStream(requireHttp2Stream(streamId));
                StreamPayload prev = channel.attr(StreamPayload.KEY).get();
                try {
                    if (payload.data().isReadable()) {
                        /** A valid stream data will trigger an rpc invoke */
                        channel.attr(StreamPayload.KEY).set(payload);
                        ctx.fireChannelRead(payload.data());
                    }
                } catch (Throwable e) {
                    logger.warn("Exception in onHeadersRead, streamId:" + streamId, e);
                    throw Http2Exception.streamError(streamId, Http2Error.INTERNAL_ERROR, e, StringUtils.nullToEmpty(e.getMessage()));
                } finally {
                    channel.attr(StreamPayload.KEY).set(prev);
                }
                return;
            }

            CharSequence path = headers.path();
            if (path == null) {
                respondWithHttpError(ctx, streamId, "Request path is missing, expect patten '/service/method'");
                return;
            }

            if (path.charAt(0) != '/') {
                respondWithHttpError(ctx, streamId, String.format("Expected path to start with /: %s", path));
                return;
            }

            if (!path.toString().equals(Constants.EVENT_PATH)) {
                // Verify that the Content-Type is correct in the request.
                CharSequence contentType = headers.get(Constants.HTTP2_CONTENT_TYPE_KEY);
                if (contentType == null) {
                    respondWithHttpError(ctx, streamId, "Content-Type is missing from the request");
                    return;
                }

                String contentTypeString = contentType.toString();
                if (!isDubboContentType(contentTypeString)) {
                    respondWithHttpError(ctx, streamId, String.format("Content-Type '%s' is not supported", contentTypeString));
                    return;
                }

                if (!Constants.POST_KEY.equals(headers.method().toString()) && !Constants.GET_KEY.equals(headers.method().toString())) {
                    respondWithHttpError(ctx, streamId, String.format("Method '%s' is not supported", headers.method()));
                    return;
                }
            }

            Http2Stream http2Stream = requireHttp2Stream(streamId);
            StreamPayload payload = new StreamPayload(http2Stream, headers, this, streamKey);
            payload.streamId(streamId).endOfStream(endStream);
            http2Stream.setProperty(streamKey, payload);

            if (logger.isDebugEnabled()) {
                logger.debug("received headers:" + headers
                        + ", streamId:" + streamId
                        + ", channel:" + ctx.channel()
                        + ", hash:" + System.identityHashCode(ctx.channel()));
            }
        } catch (Exception e) {
            logger.warn("Unexpected onHeaderRead, streamId:" + streamId, e);
            throw Http2Exception.streamError(
                    streamId, Http2Error.INTERNAL_ERROR, e, StringUtils.nullToEmpty(e.getMessage()));
        }
    }

    protected void onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding, boolean endOfStream)
            throws Http2Exception {
        try {
            Channel channel = ctx.channel();
            StreamPayload payload = payloadOfStream(requireHttp2Stream(streamId));
            if (payload == null || payload.streamId() != streamId) {
                // never happen ？
                throw new RemotingException((InetSocketAddress) channel.localAddress(), (InetSocketAddress) channel.remoteAddress(),
                        "received remote data from streamId:" + streamId + ", but not found payload.");
            }

            if (payload.data() == null) {
                payload.data(ctx.alloc().buffer());
            }
            payload.endOfStream(endOfStream).data().writeBytes(data);

            if (logger.isDebugEnabled()) {
                logger.debug("received data streamId:" + streamId + " length:" + data.readableBytes() + " endOfStream:" + endOfStream);
            }

            if (endOfStream) {
                StreamPayload prev = channel.attr(StreamPayload.KEY).get();
                try {
                    /** A valid stream data will trigger an rpc invoke */
                    channel.attr(StreamPayload.KEY).set(payload);
                    ctx.fireChannelRead(payload.data());
                } finally {
                    channel.attr(StreamPayload.KEY).set(prev);
                }
            }
        } catch (Throwable e) {
            logger.warn("Exception in onDataRead, streamId:" + streamId, e);
            throw Http2Exception.streamError(
                    streamId, Http2Error.INTERNAL_ERROR, e, StringUtils.nullToEmpty(e.getMessage()));
        }
    }

    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        super.handlerAdded(ctx);
        sendInitialConnectionWindow();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        try {
            this.userEventTriggered(ctx, AbstractHttp2Initializer.Http2ConnectionActiveEvent.INSTANCE);
        } finally {
            super.channelActive(ctx);
            sendInitialConnectionWindow();
        }
    }

    @Override
    public final void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        Http2Exception embedded = getEmbeddedHttp2Exception(cause);
        if (embedded == null) {
            // There was no embedded Http2Exception, assume it's a connection error. Subclasses are
            // responsible for storing the appropriate status and shutting down the connection.
            onError(ctx, false, cause);
        } else {
            super.exceptionCaught(ctx, cause);
        }
    }

    /**
     * Sends initial connection window to the remote endpoint if necessary.
     */
    private void sendInitialConnectionWindow() throws Http2Exception {
        if (ctx.channel().isActive() && initialConnectionWindow > 0) {
            Http2Stream connectionStream = connection().connectionStream();
            int currentSize = connection().local().flowController().windowSize(connectionStream);
            int delta = initialConnectionWindow - currentSize;
            decoder().flowController().incrementWindowSize(connectionStream, delta);
            initialConnectionWindow = -1;
            ctx.flush();
        }
    }

    protected Http2Stream requireHttp2Stream(int streamId) {
        Http2Stream stream = connection().stream(streamId);
        return stream;
    }

    protected StreamPayload payloadOfStream(Http2Stream stream) {
        return stream == null ? null : (StreamPayload) stream.getProperty(streamKey);
    }

    private void respondWithHttpError(
            ChannelHandlerContext ctx, int streamId, String message) {

        // http2 header
        Http2Headers header = new DefaultHttp2Headers();
        header.status(Constants.HTTP2_OK_STATUS);
        header.add(Constants.HTTP2_CONTENT_TYPE_KEY, Constants.HTTP2_DUBBO_CONTENT_TYPE);

        ChannelPromise promise = ctx.newPromise();
        encoder().writeHeaders(ctx, streamId, header, 0, false, promise.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    closeStreamWhenComplete(promise, streamId);
                }
            }
        }));

        // http2 trailer
        Http2Headers trailer = new DefaultHttp2Headers();
        trailer.add(Constants.HTTP2_DUBBO_RPC_MESSAGE, message);

        encoder().writeHeaders(ctx, streamId, trailer, 0, true, promise.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                closeStreamWhenComplete(promise, streamId);
            }
        }));
    }

    protected void closeStreamWhenComplete(ChannelPromise promise, int streamId) throws Http2Exception {

        Http2Stream stream = requireHttp2Stream(streamId);
        if (stream == null) return;

        final StreamPayload payload = payloadOfStream(stream);
        promise.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                if (payload != null) payload.complete();
            }
        });
    }

    protected final ChannelHandlerContext ctx() {
        return ctx;
    }

    protected boolean isDubboContentType(String contentType) {
        if (contentType == null) {
            return false;
        }

        contentType = contentType.toLowerCase();
        if (!contentType.startsWith(Constants.HTTP2_DUBBO_CONTENT_TYPE)) {
            return false;
        }

        if (contentType.length() == Constants.HTTP2_DUBBO_CONTENT_TYPE.length()) {
            return true;
        }

        char nextChar = contentType.charAt(Constants.HTTP2_DUBBO_CONTENT_TYPE.length());
        return nextChar == '+' || nextChar == ';';
    }

    protected String debuggerMessage(Channel channel) {
        String handlers = "";
        if (logger.isDebugEnabled()) {
            Map<String, ChannelHandler> handlerMap = channel.pipeline().toMap();
            StringBuilder names = new StringBuilder();
            if (handlerMap != null && !handlerMap.isEmpty()) {
                Iterator<Map.Entry<String, ChannelHandler>> iterator = handlerMap.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, ChannelHandler> handlerEntry = iterator.next();
                    String name = handlerEntry.getKey();
                    ChannelHandler handler = handlerEntry.getValue();

                    if (names.length() > 0) {
                        names.append("->");
                    }
                    names.append(StringUtils.nullToEmpty(name)).append(":").append(handler.getClass().getName());
                }
            }
            handlers = names.toString();
        }
        return handlers;
    }
}
