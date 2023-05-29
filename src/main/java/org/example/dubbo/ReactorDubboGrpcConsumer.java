/*
 *
 *   Licensed to the Apache Software Foundation (ASF) under one or more
 *   contributor license agreements.  See the NOTICE file distributed with
 *   this work for additional information regarding copyright ownership.
 *   The ASF licenses this file to You under the Apache License, Version 2.0
 *   (the "License"); you may not use this file except in compliance with
 *   the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */

package org.example.voicechat.client;

import com.google.protobuf.ByteString;
import org.example.voicechat.client.rpc.ReactorDubboVoiceChatGrpc;
import org.example.voicechat.client.rpc.VoiceChatRequest;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import reactor.core.publisher.Flux;

public class ReactorDubboGrpcConsumer {

    public static void main(String[] args) throws Exception {
        ClassPathXmlApplicationContext context =
                new ClassPathXmlApplicationContext("spring/reactor-dubbo-grpc-consumer.xml");
        context.start();

        /**
         * greeter sample
         */
        System.out.println("-------- Start simple unary call test -------- ");
        ReactorDubboVoiceChatGrpc.IReactorVoiceChat voiceChat = (ReactorDubboVoiceChatGrpc.IReactorVoiceChat) context.getBean("voiceChat");

        voiceChat
                .chat(Flux.range(1, 10)
                        .map(num -> VoiceChatRequest.newBuilder().setCallId("123456").setData(ByteString.copyFrom(num.toString().getBytes())).build())
                ).subscribe(reply -> System.out.println("Result: " + reply));

        System.out.println("-------- End simple unary call test -------- \n\n\n");

        System.in.read();
    }
}
