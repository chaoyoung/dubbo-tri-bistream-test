/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.example.dubbo;

import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.ServiceConfig;
import org.apache.dubbo.config.bootstrap.DubboBootstrap;
import org.example.dubbo.chat.DubboTripleVoiceChatImpl;
import org.example.dubbo.chat.VoiceChat;
import org.example.dubbo.util.EmbeddedZooKeeper;

public class DubboTripleVoiceChatProvider {

    public static void main(String[] args) {
        new EmbeddedZooKeeper(2181, false).start();

        ApplicationConfig applicationConfig = new ApplicationConfig("tri-stub-server");
        applicationConfig.setQosEnable(false);
        applicationConfig.setProtocol("tri");
        ProtocolConfig protocolConfig = new ProtocolConfig("tri", 50051);
        protocolConfig.setSerialization("protobuf");

        ServiceConfig<VoiceChat> service = new ServiceConfig<>();
        service.setInterface(VoiceChat.class);
        service.setRef(new DubboTripleVoiceChatImpl());
        service.setPath("org.example.dubbo.chat.VoiceChat");

        DubboBootstrap bootstrap = DubboBootstrap.getInstance();
        bootstrap.application(applicationConfig)
                .registry(new RegistryConfig("zookeeper://localhost:2181"))
                .protocol(protocolConfig)
                .service(service)
                .start();

        System.out.println("Dubbo triple streaming server started, port=" + 50051);
    }
}
