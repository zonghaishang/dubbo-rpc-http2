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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpClientUpgradeHandler;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.Http2ClientUpgradeCodec;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import javax.net.ssl.SSLException;
import java.security.cert.CertificateException;

public class NettyHttp2ClientInitalizer extends AbstractHttp2Initializer<SocketChannel> {

    private static final Logger logger = LoggerFactory.getLogger(NettyHttp2ClientInitalizer.class);

    public NettyHttp2ClientInitalizer(URL url) throws SSLException, CertificateException {
        super(url);
    }

    /**
     * Configure the pipeline for TLS NPN negotiation to HTTP/2.
     */
    @Override
    protected void configureSsl(SocketChannel ch) {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast(sslCtx.newHandler(ch.alloc()));
        configureEndOfPipeline(ch.pipeline());
    }

    /**
     * Configure the pipeline for a cleartext upgrade from HTTP to HTTP/2.
     */
    @Override
    protected void configureClearText(SocketChannel ch) {
        String negotiate = url.getParameter(Constants.NEGOTIATE_KEY, Constants.DEFAULT_NEGOTIATE);
        if (negotiate.equals(Constants.DEFAULT_NEGOTIATE)) {
            configureEndOfPipeline(ch.pipeline());
        } else if (negotiate.equals(Constants.NEGOTIATE_HTTP_1_0)) {
            HttpClientCodec httpClientCodec = new HttpClientCodec();
            Http2ClientUpgradeCodec upgradeCodec = new Http2ClientUpgradeCodec(NettyHttp2ClientHandler.newHandler(url));
            HttpClientUpgradeHandler upgrader = new HttpClientUpgradeHandler(httpClientCodec, upgradeCodec, maxHttpContentLength);
            ch.pipeline().addLast(httpClientCodec, upgrader, new UpgradeRequestHandler());
        } else {
            throw new RuntimeException("Not support negotiate type '" + negotiate + "' for http2 protocol.");
        }
    }

    protected SslContextBuilder getSSLContextBuilder(URL url, String certificate, String privateKey) throws CertificateException {
        return SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE);
    }

    private final class UpgradeRequestHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            DefaultFullHttpRequest upgradeRequest =
                    new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
            ctx.writeAndFlush(upgradeRequest);
            ctx.pipeline().remove(this);
            ctx.fireChannelActive();
        }
    }
}
