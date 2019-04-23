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

import java.io.Serializable;


@SuppressWarnings("serial")
public class CustomArgument implements Serializable {

    TypeOfEnum typeOfEnum;
    String name;

    public CustomArgument() {
    }

    public CustomArgument(TypeOfEnum typeOfEnum, String name) {
        super();
        this.typeOfEnum = typeOfEnum;
        this.name = name;
    }

    public TypeOfEnum getTypeOfEnum() {
        return typeOfEnum;
    }

    public void setTypeOfEnum(TypeOfEnum typeOfEnum) {
        this.typeOfEnum = typeOfEnum;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}