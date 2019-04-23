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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpClientUpgradeHandler;
import io.netty.handler.codec.http.HttpServerUpgradeHandler;
import io.netty.handler.codec.http2.Http2CodecUtil;
import io.netty.handler.codec.http2.Http2ConnectionPrefaceAndSettingsFrameWrittenEvent;
import io.netty.handler.codec.http2.Http2SecurityUtil;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.util.AsciiString;

import javax.net.ssl.SSLException;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.cert.CertificateException;

public abstract class AbstractHttp2Initializer<C extends Channel> extends ChannelInitializer<C> {

    private static final Logger logger = LoggerFactory.getLogger(AbstractHttp2Initializer.class);
    protected SslContext sslCtx;
    protected int maxHttpContentLength;
    protected URL url;

    public AbstractHttp2Initializer(URL url) throws SSLException, CertificateException {

        this.url = url;
        this.maxHttpContentLength = url.getParameter(Constants.MAX_HTTP_CONTENT_BYTES_KEY, Constants.MAX_HTTP_CONTENT_BYTES);

        if (url.getParameter(Constants.SSL_ENABLE_KEY, false)) {
            long now = System.currentTimeMillis();
            String certificate = url.getParameter(Constants.SSL_CERTIFICATE_KEY);
            String privateKey = url.getParameter(Constants.SSL_PRIVATE_KEY);
            SslContextBuilder sslContextBuilder = getSSLContextBuilder(url, certificate, privateKey);
            this.sslCtx = sslContextBuilder.sslProvider(OpenSsl.isAlpnSupported() ? SslProvider.OPENSSL : SslProvider.JDK)
                    /* NOTE: the cipher filter may not include all ciphers required by the HTTP/2 specification. */
                    .ciphers(Http2SecurityUtil.CIPHERS, SupportedCipherSuiteFilter.INSTANCE)
                    .applicationProtocolConfig(new ApplicationProtocolConfig(
                            ApplicationProtocolConfig.Protocol.ALPN,
                            ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                            ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                            ApplicationProtocolNames.HTTP_2,
                            ApplicationProtocolNames.HTTP_1_1)).build();
            logger.info(url.getParameter(Constants.SIDE_KEY, "") + " ssl initailized: " + (System.currentTimeMillis() - now) + "ms");
        }
    }

    protected SslContextBuilder getSSLContextBuilder(URL url, String certificate, String privateKey) throws CertificateException {
        SslContextBuilder sslContextBuilder;
        if (SslShouldBeUsed(certificate, privateKey)) {
            sslContextBuilder = SslContextBuilder.forServer(new File(certificate), new File(privateKey), url.getParameter(Constants.SSL_PRIVATE_KEY_PASSWORD));
        } else {
            SelfSignedCertificate ssc = new SelfSignedCertificate();
            sslContextBuilder = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey());
            logger.warn("Not found certificate" + StringUtils.nullToEmpty(certificate)
                    + " or private key file " + StringUtils.nullToEmpty(privateKey) + ", "
                    + "will be created an temporary self-signed certificate for testing purposes, "
                    + "It is not recommended to use in production environment.");
        }
        return sslContextBuilder;
    }

    @Override
    protected void initChannel(C channel) throws Exception {
        if (sslCtx != null) {
            configureSsl(channel);
        } else {
            configureClearText(channel);
        }
        channel.pipeline().addLast(new Http2NegotiatorHandler());
    }

    protected void negotiationCompleted(C channel) {

    }

    // ssl configured ?
    protected boolean SslShouldBeUsed(String certificate, String privateKey) {
        try {
            return StringUtils.isNotEmpty(certificate)
                    && StringUtils.isNotEmpty(privateKey)
                    && Files.exists(Paths.get(certificate))
                    && Files.exists(Paths.get(privateKey));
        } catch (Exception e) {
            logger.error("detect ssl failed", e);
            return false;
        }
    }

    protected abstract void configureSsl(C channel);

    protected abstract void configureClearText(C channel);

    protected void configureEndOfPipeline(ChannelPipeline pipeline) {
        pipeline.addLast(NettyHttp2ClientHandler.newHandler(url));
    }

    public static class Http2ConnectionActiveEvent {
        static final Http2ConnectionActiveEvent INSTANCE =
                new Http2ConnectionActiveEvent();

        private Http2ConnectionActiveEvent() {
        }
    }

    /**
     * Class that logs any User Events triggered on this channel.
     */
    private class Http2NegotiatorHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (logger.isDebugEnabled()) {
                logger.debug("Debug user event: " + evt);
            }
            // http/1.x client upgrade event
            if (evt == HttpClientUpgradeHandler.UpgradeEvent.UPGRADE_SUCCESSFUL) {
                ctx.pipeline().remove(this);
                negotiationCompleted((C) ctx.channel());
            }
            // if use ssl, both side should be http2 protocol
            else if (evt == SslHandshakeCompletionEvent.SUCCESS) {
                ctx.pipeline().remove(this);
                negotiationCompleted((C) ctx.channel());
            }
            // http/1.x server upgrade event
            else if (evt instanceof HttpServerUpgradeHandler.UpgradeEvent) {
                HttpServerUpgradeHandler.UpgradeEvent upgradeEvent = (HttpServerUpgradeHandler.UpgradeEvent) evt;
                if (AsciiString.contentEquals(Http2CodecUtil.HTTP_UPGRADE_PROTOCOL_NAME, upgradeEvent.protocol())) {
                    ctx.pipeline().remove(this);
                    negotiationCompleted((C) ctx.channel());
                }
            } else if (evt instanceof AbstractHttp2Initializer.Http2ConnectionActiveEvent) {
                ctx.pipeline().remove(this);
                negotiationCompleted((C) ctx.channel());
            }
            // both side is http2 protocol
            else if (evt instanceof Http2ConnectionPrefaceAndSettingsFrameWrittenEvent) {
                // Send prefance immediately
                ctx.flush();
                ctx.pipeline().remove(this);
                negotiationCompleted((C) ctx.channel());
            } else if (evt == HttpClientUpgradeHandler.UpgradeEvent.UPGRADE_REJECTED) {
                logger.error("HTTP/2 upgrade rejected , evt: " + evt);
                ctx.close();
            }
            super.userEventTriggered(ctx, evt);
        }
    }
}
