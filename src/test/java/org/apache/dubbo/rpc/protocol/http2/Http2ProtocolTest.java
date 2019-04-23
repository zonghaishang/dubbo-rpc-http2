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


import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.protocol.http2.support.NonSerialized;
import org.apache.dubbo.rpc.protocol.http2.support.TypeOfEnum;
import org.apache.dubbo.rpc.protocol.http2.support.UnitTestRemoteService;
import org.apache.dubbo.rpc.protocol.http2.support.UnitTestRemoteServiceImpl;
import org.apache.dubbo.rpc.protocol.http2.support.UnitTestService;
import org.apache.dubbo.rpc.protocol.http2.support.UnitTestServiceImpl;
import org.apache.dubbo.rpc.service.EchoService;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * <code>ProxiesTest</code>
 */

public class Http2ProtocolTest {
    private Protocol protocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();
    private ProxyFactory proxy = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();

    @Test
    public void testHttp2Protocol() throws Exception {
        UnitTestService service = new UnitTestServiceImpl();
        URL url = URL.valueOf("http2://127.0.0.1:" + NetUtils.getAvailablePort() + "/" + UnitTestService.class.getName() + "?codec=exchange");
        protocol.export(proxy.getInvoker(service, UnitTestService.class, url));
        service = proxy.getProxy(protocol.refer(UnitTestService.class, url));
        assertEquals(service.getSize(new String[]{"", "", ""}), 3);
    }

    @Test
    public void testHttp2ProtocolWithSsl() throws Exception {
        UnitTestService service = new UnitTestServiceImpl();
        URL url = URL.valueOf("http2://127.0.0.1:" + NetUtils.getAvailablePort() + "/" + UnitTestService.class.getName() + "?codec=exchange&ssl=true");
        protocol.export(proxy.getInvoker(service, UnitTestService.class, url));
        service = proxy.getProxy(protocol.refer(UnitTestService.class, url));
        assertEquals(service.getSize(new String[]{"", "", ""}), 3);
    }

    @Test
    public void testHttp2ProtocolWithHttp1_0() throws Exception {
        UnitTestService service = new UnitTestServiceImpl();
        URL url = URL.valueOf("http2://127.0.0.1:" + NetUtils.getAvailablePort() + "/" + UnitTestService.class.getName() + "?codec=exchange&negotiate=http/1.0");
        protocol.export(proxy.getInvoker(service, UnitTestService.class, url));
        service = proxy.getProxy(protocol.refer(UnitTestService.class, url));
        /**
         * Keep some time to upgrade the connection，
         * This problem does not exist through the registry
         * because service discovery is not called immediately
         */
        Thread.sleep(1000);
        assertEquals(service.getSize(new String[]{"", "", ""}), 3);
    }

    @Test
    public void testHttp2Protocol2() throws Exception {
        UnitTestService service = new UnitTestServiceImpl();
        URL url = URL.valueOf("http2://127.0.0.1:" + NetUtils.getAvailablePort() + "/" + UnitTestService.class.getName());
        protocol.export(proxy.getInvoker(service, UnitTestService.class, url));
        service = proxy.getProxy(protocol.refer(UnitTestService.class, url));
        assertEquals(service.enumlength(new TypeOfEnum[]{}), TypeOfEnum.Lower);
        assertEquals(service.getSize(null), -1);
        assertEquals(service.getSize(new String[]{"", "", ""}), 3);
        Map<String, String> map = new HashMap<String, String>();
        map.put("aa", "bb");
        Set<String> set = service.keys(map);
        assertEquals(set.size(), 1);
        assertEquals(set.iterator().next(), "aa");
        service.invoke(url.toServiceString(), "invoke");

        service = proxy.getProxy(protocol.refer(UnitTestService.class, url));
        // test http2 client
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < 1024 * 32 + 32; i++) buf.append('A');
        System.out.println(service.stringLength(buf.toString()));

        // cast to EchoService
        EchoService echo = proxy.getProxy(protocol.refer(EchoService.class, url));
        assertEquals(echo.$echo(buf.toString()), buf.toString());
        assertEquals(echo.$echo("test"), "test");
        assertEquals(echo.$echo("abcdefg"), "abcdefg");
        assertEquals(echo.$echo(1234), 1234);
    }

    @Test
    public void testHttp2ProtocolMultiService() throws Exception {
        UnitTestService service = new UnitTestServiceImpl();
        URL url = URL.valueOf("http2://127.0.0.1:" + NetUtils.getAvailablePort() + "/" + UnitTestService.class.getName());
        protocol.export(proxy.getInvoker(service, UnitTestService.class, url));
        service = proxy.getProxy(protocol.refer(UnitTestService.class, url));

        UnitTestRemoteService remote = new UnitTestRemoteServiceImpl();
        URL url0 = URL.valueOf("http2://127.0.0.1:" + NetUtils.getAvailablePort() + "/" + UnitTestRemoteService.class.getName());
        protocol.export(proxy.getInvoker(remote, UnitTestRemoteService.class, url0));
        remote = proxy.getProxy(protocol.refer(UnitTestRemoteService.class, url0));

        service.sayHello("world");

        // test netty client
        assertEquals("world", service.echo("world"));
        assertEquals("hello world@" + UnitTestRemoteServiceImpl.class.getName(), remote.sayHello("world"));

        EchoService serviceEcho = (EchoService) service;
        assertEquals(serviceEcho.$echo("test"), "test");

        EchoService remoteEecho = (EchoService) remote;
        assertEquals(remoteEecho.$echo("ok"), "ok");
    }

    @Test
    public void testPerm() throws Exception {
        UnitTestService service = new UnitTestServiceImpl();
        URL url = URL.valueOf("http2://127.0.0.1:" + NetUtils.getAvailablePort() + "/" + UnitTestService.class.getName() + "?codec=exchange");
        protocol.export(proxy.getInvoker(service, UnitTestService.class, url));
        service = proxy.getProxy(protocol.refer(UnitTestService.class, url));
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++)
            service.getSize(new String[]{"", "", ""});
        System.out.println("take:" + (System.currentTimeMillis() - start));
    }

    @Test
    public void testNonSerializedParameter() throws Exception {
        UnitTestService service = new UnitTestServiceImpl();
        URL url = URL.valueOf("http2://127.0.0.1:" + NetUtils.getAvailablePort() + "/" + UnitTestService.class.getName() + "?codec=exchange");
        protocol.export(proxy.getInvoker(service, UnitTestService.class, url));
        service = proxy.getProxy(protocol.refer(UnitTestService.class, url));
        try {
            service.nonSerializedParameter(new NonSerialized());
            Assert.fail();
        } catch (RpcException e) {
            Assert.assertTrue(e.getMessage().contains("org.apache.dubbo.rpc.protocol.http2.support.NonSerialized must implement java.io.Serializable"));
        }
    }

    @Test
    public void testReturnNonSerialized() throws Exception {
        UnitTestService service = new UnitTestServiceImpl();
        URL url = URL.valueOf("http2://127.0.0.1:" + NetUtils.getAvailablePort() + "/" + UnitTestService.class.getName() + "?codec=exchange");
        protocol.export(proxy.getInvoker(service, UnitTestService.class, url));
        service = proxy.getProxy(protocol.refer(UnitTestService.class, url));
        try {
            service.returnNonSerialized();
            Assert.fail();
        } catch (RpcException e) {
            Assert.assertTrue(e.getMessage().contains("org.apache.dubbo.rpc.protocol.http2.support.NonSerialized must implement java.io.Serializable"));
        }
    }
}