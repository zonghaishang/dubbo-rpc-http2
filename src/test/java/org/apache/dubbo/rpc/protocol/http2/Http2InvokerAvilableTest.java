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
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.remoting.exchange.ExchangeClient;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.protocol.dubbo.DubboInvoker;
import org.apache.dubbo.rpc.protocol.http2.support.Http2ProtocolUtils;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.fail;

/**
 * Check available status for dubboInvoker
 */
public class Http2InvokerAvilableTest {
    private static Http2Protocol protocol = Http2Protocol.getHttp2Protocol();
    private static ProxyFactory proxy = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();

    @Test
    public void test_Normal_available() {
        URL url = URL.valueOf("http2://127.0.0.1:" + NetUtils.getAvailablePort() + "/org.apache.dubbo.rpc.protocol.http2.IDemoService");
        Http2ProtocolUtils.export(new DemoServiceImpl(), IDemoService.class, url);

        DubboInvoker<?> invoker = (DubboInvoker<?>) protocol.refer(IDemoService.class, url);
        Assert.assertEquals(true, invoker.isAvailable());
        invoker.destroy();
        Assert.assertEquals(false, invoker.isAvailable());
    }

    @Test
    public void test_Normal_ChannelReadOnly() throws Exception {
        URL url = URL.valueOf("http2://127.0.0.1:" + NetUtils.getAvailablePort() + "/org.apache.dubbo.rpc.protocol.http2.IDemoService");
        Http2ProtocolUtils.export(new DemoServiceImpl(), IDemoService.class, url);

        DubboInvoker<?> invoker = (DubboInvoker<?>) protocol.refer(IDemoService.class, url);
        Assert.assertEquals(true, invoker.isAvailable());

        getClients(invoker)[0].setAttribute(Constants.CHANNEL_ATTRIBUTE_READONLY_KEY, Boolean.TRUE);

        Assert.assertEquals(false, invoker.isAvailable());

        // reset status since connection is shared among invokers
        getClients(invoker)[0].removeAttribute(Constants.CHANNEL_ATTRIBUTE_READONLY_KEY);
    }

    @Test
    public void test_NoInvokers() throws Exception {
        URL url = URL.valueOf("http2://127.0.0.1:" + NetUtils.getAvailablePort() + "?connections=1");
        Http2ProtocolUtils.export(new DemoServiceImpl(), IDemoService.class, url);

        DubboInvoker<?> invoker = (DubboInvoker<?>) protocol.refer(IDemoService.class, url);

        ExchangeClient[] clients = getClients(invoker);
        clients[0].close();
        Assert.assertEquals(false, invoker.isAvailable());

    }

    @Test
    public void test_Lazy_ChannelReadOnly() throws Exception {
        URL url = URL.valueOf("http2://127.0.0.1:" + NetUtils.getAvailablePort() + "/org.apache.dubbo.rpc.protocol.http2.IDemoService?lazy=true&connections=1");
        Http2ProtocolUtils.export(new DemoServiceImpl(), IDemoService.class, url);

        DubboInvoker<?> invoker = (DubboInvoker<?>) protocol.refer(IDemoService.class, url);
        Assert.assertEquals(true, invoker.isAvailable());

        try {
            getClients(invoker)[0].setAttribute(Constants.CHANNEL_ATTRIBUTE_READONLY_KEY, Boolean.TRUE);
            fail();
        } catch (IllegalStateException e) {

        }
        //invoke method --> init client
        IDemoService service = (IDemoService) proxy.getProxy(invoker);
        Assert.assertEquals("ok", service.get());

        Assert.assertEquals(true, invoker.isAvailable());
        getClients(invoker)[0].setAttribute(Constants.CHANNEL_ATTRIBUTE_READONLY_KEY, Boolean.TRUE);
        Assert.assertEquals(false, invoker.isAvailable());
    }

    private ExchangeClient[] getClients(DubboInvoker<?> invoker) throws Exception {
        Field field = DubboInvoker.class.getDeclaredField("clients");
        field.setAccessible(true);
        ExchangeClient[] clients = (ExchangeClient[]) field.get(invoker);
        Assert.assertEquals(1, clients.length);
        return clients;
    }

    public class DemoServiceImpl implements IDemoService {
        @Override
        public String demo() {
            return "demo";
        }

        public String get() {
            return "ok";
        }
    }

    public class DemoServiceImpl0 implements IDemoService {
        @Override
        public String demo() {
            return "demo0";
        }

        public String get() {
            return "ok";
        }
    }
}