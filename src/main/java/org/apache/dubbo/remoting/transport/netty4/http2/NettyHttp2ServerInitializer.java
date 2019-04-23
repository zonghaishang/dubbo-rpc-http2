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

import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerUpgradeHandler;
import io.netty.handler.codec.http2.CleartextHttp2ServerUpgradeHandler;
import io.netty.handler.codec.http2.Http2CodecUtil;
import io.netty.handler.codec.http2.Http2ServerUpgradeCodec;
import io.netty.util.AsciiString;

import javax.net.ssl.SSLException;
import java.security.cert.CertificateException;

public class NettyHttp2ServerInitializer extends AbstractHttp2Initializer<NioSocketChannel> {

    private static final Logger logger = LoggerFactory.getLogger(NettyHttp2ServerInitializer.class);
    final HttpServerUpgradeHandler.UpgradeCodecFactory upgradeCodecFactory = new HttpServerUpgradeHandler.UpgradeCodecFactory() {
        @Override
        public HttpServerUpgradeHandler.UpgradeCodec newUpgradeCodec(CharSequence protocol) {
            if (AsciiString.contentEquals(Http2CodecUtil.HTTP_UPGRADE_PROTOCOL_NAME, protocol)) {
                return new Http2ServerUpgradeCodec(NettyHttp2ServerHandler.newHandler(url));
            } else {
                return null;
            }
        }
    };

    public NettyHttp2ServerInitializer(URL url) throws SSLException, CertificateException {
        super(url);
    }

    /**
     * Configure the pipeline for TLS NPN negotiation to HTTP/2.
     */
    @Override
    protected void configureSsl(NioSocketChannel ch) {
        ch.pipeline().addLast(sslCtx.newHandler(ch.alloc()), NettyHttp2ServerHandler.newHandler(url));
    }

    /**
     * Configure the pipeline for a cleartext upgrade from HTTP to HTTP/2.0
     */
    @Override
    protected void configureClearText(NioSocketChannel channel) {
        String negotiate = url.getParameter(Constants.NEGOTIATE_KEY, Constants.DEFAULT_NEGOTIATE);
        if (negotiate.equals(Constants.DEFAULT_NEGOTIATE)) {
            channel.pipeline().addLast(NettyHttp2ServerHandler.newHandler(url));
        } else if (negotiate.equals(Constants.NEGOTIATE_HTTP_1_0)) {
            final ChannelPipeline p = channel.pipeline();
            final HttpServerCodec httpServerCodec = new HttpServerCodec();
            final HttpServerUpgradeHandler upgradeHandler = new HttpServerUpgradeHandler(httpServerCodec, upgradeCodecFactory);
            final CleartextHttp2ServerUpgradeHandler cleartextHttp2ServerUpgradeHandler =
                    new CleartextHttp2ServerUpgradeHandler(httpServerCodec, upgradeHandler,
                            NettyHttp2ServerHandler.newHandler(url));

            channel.pipeline().addLast(cleartextHttp2ServerUpgradeHandler);
        } else {
            throw new RuntimeException("Not support negotiate type '" + negotiate + "' for http2 protocol.");
        }
    }
}
