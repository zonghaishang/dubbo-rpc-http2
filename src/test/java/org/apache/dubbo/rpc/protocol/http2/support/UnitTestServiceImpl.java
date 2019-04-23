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
package org.apache.dubbo.rpc.protocol.http2.support;

import org.apache.dubbo.rpc.RpcContext;

import java.util.Map;
import java.util.Set;

/**
 * UnitTestServiceImpl
 */

public class UnitTestServiceImpl implements UnitTestService {
    public UnitTestServiceImpl() {
        super();
    }

    public void sayHello(String name) {
        System.out.println("hello " + name);
    }

    public String echo(String text) {
        return text;
    }

    public Map echo(Map map) {
        return map;
    }

    public long timestamp() {
        return System.currentTimeMillis();
    }

    public String getThreadName() {
        return Thread.currentThread().getName();
    }

    public int getSize(String[] strs) {
        if (strs == null)
            return -1;
        return strs.length;
    }

    public int getSize(Object[] os) {
        if (os == null)
            return -1;
        return os.length;
    }

    public Object invoke(String service, String method) throws Exception {
        System.out.println("RpcContext.getContext().getRemoteHost()=" + RpcContext.getContext().getRemoteHost());
        return service + ":" + method;
    }

    public TypeOfEnum enumlength(TypeOfEnum... typeOfEnums) {
        if (typeOfEnums.length == 0)
            return TypeOfEnum.Lower;
        return typeOfEnums[0];
    }

    public TypeOfEnum enumlength(TypeOfEnum typeOfEnum) {
        return typeOfEnum;
    }

    public int stringLength(String str) {
        return str.length();
    }

    public String get(CustomArgument arg1) {
        return arg1.toString();
    }

    public byte getbyte(byte arg) {
        return arg;
    }

    public Person gerPerson(Person person) {
        return person;
    }

    public Set<String> keys(Map<String, String> map) {
        return map == null ? null : map.keySet();
    }

    public void nonSerializedParameter(NonSerialized ns) {
    }

    public NonSerialized returnNonSerialized() {
        return new NonSerialized();
    }
}