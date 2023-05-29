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

import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.stream.StreamObserver;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.bootstrap.DubboBootstrap;
import org.example.dubbo.chat.VoiceChat;
import org.example.dubbo.chat.VoiceChatRequest;
import org.example.dubbo.chat.VoiceChatResponse;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DubboTripleVoiceChatConsumer {

    private static final ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(200, new NamedThreadFactory("chat-client-stream", false));

    public static void main(String[] args) throws Exception {
        DubboBootstrap bootstrap = DubboBootstrap.getInstance();
        ReferenceConfig<VoiceChat> ref = new ReferenceConfig<>();
        ref.setInterface(VoiceChat.class);
        ref.setProtocol(CommonConstants.TRIPLE);
        ref.setProxy(CommonConstants.NATIVE_STUB);
//        ref.setUrl("grpc://localhost:50051/org.example.dubbo.chat.VoiceChat");

        ApplicationConfig applicationConfig = new ApplicationConfig("tri-stub-consumer");
        applicationConfig.setQosEnable(false);
        applicationConfig.setProtocol(CommonConstants.TRIPLE);
        bootstrap.application(applicationConfig).reference(ref).registry(new RegistryConfig("zookeeper://localhost:2181")).start();
        VoiceChat voiceChat = ref.get();
        for (int i = 0; i < 200; i++) {
            StreamObserver<VoiceChatRequest> requestStreamObserver = voiceChat.chat(new SampleStreamObserver());
            executorService.scheduleAtFixedRate(() -> {
                VoiceChatRequest request = VoiceChatRequest.newBuilder().setData(ByteString.copyFrom(new byte[160])).build();
                requestStreamObserver.onNext(request);
            }, 100L, 100L, TimeUnit.MILLISECONDS);
            Thread.sleep(100L);
        }
    }

    private static class SampleStreamObserver implements StreamObserver<VoiceChatResponse> {
        int n = 0;
        @Override
        public void onNext(VoiceChatResponse data) {
            n++;
            if (n % 10 == 0) {
                log.info("callId: {} : reply data size: {}", data.getCallId(), data.getData().size());
            }
        }

        @Override
        public void onError(Throwable throwable) {
            log.error("voice chat onError", throwable);
            throwable.printStackTrace();
        }

        @Override
        public void onCompleted() {
            log.info("voice chat completed");
        }
    }

}