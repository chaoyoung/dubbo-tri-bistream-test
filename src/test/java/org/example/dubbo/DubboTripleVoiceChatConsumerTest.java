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

import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioSystem;
import javax.sound.sampled.SourceDataLine;
import javax.sound.sampled.TargetDataLine;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DubboTripleVoiceChatClient {
    private static final AudioFormat.Encoding encoding = new AudioFormat.Encoding("PCM_SIGNED");
    private static final AudioFormat format = new AudioFormat(encoding, 8000, 16, 1, 2, 8000, false);//编码格式，采样率，每个样本的位数，声道，帧长（字节），帧数，是否按big-endian字节顺序存储

    private static final ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(200, new NamedThreadFactory("chat-client-stream", false));

    public static void main(String[] args) throws Exception {
        DubboBootstrap bootstrap = DubboBootstrap.getInstance();
        ReferenceConfig<VoiceChat> ref = new ReferenceConfig<>();
        ref.setInterface(VoiceChat.class);
        ref.setProtocol(CommonConstants.TRIPLE);
        ref.setProxy(CommonConstants.NATIVE_STUB);
        ref.setUrl("tri://localhost:50051/org.example.dubbo.chat.VoiceChat");

        ApplicationConfig applicationConfig = new ApplicationConfig("tri-stub-consumer");
        applicationConfig.setQosEnable(false);
        applicationConfig.setProtocol(CommonConstants.TRIPLE);
        bootstrap.application(applicationConfig).reference(ref).registry(new RegistryConfig("zookeeper://localhost:2181")).start();
        VoiceChat voiceChat = ref.get();

        TargetDataLine targetDataLine = AudioSystem.getTargetDataLine(format);
        targetDataLine.open(format);
        targetDataLine.start();

        SourceDataLine sourceDataLine = AudioSystem.getSourceDataLine(format);
        sourceDataLine.open(format);
        sourceDataLine.start();

        for (int i = 0; i < 200; i++) {
            String callId = String.valueOf(System.currentTimeMillis());
            StreamObserver<VoiceChatRequest> requestStreamObserver = voiceChat.chat(new VoiceChatStreamObserver(sourceDataLine));
            executorService.scheduleAtFixedRate(() -> {
                byte[] buffer = new byte[1600];
                int len = targetDataLine.read(buffer, 0, buffer.length);
                VoiceChatRequest request = VoiceChatRequest.newBuilder()
                        .setCallId(callId).setData(ByteString.copyFrom(buffer))
                        .build();
                requestStreamObserver.onNext(request);
            }, 100, 100, TimeUnit.MILLISECONDS);
            Thread.sleep(100L);
        }
    }

    private static class VoiceChatStreamObserver implements StreamObserver<VoiceChatResponse> {

        SourceDataLine sourceDataLine;
        int n = 0;
        public VoiceChatStreamObserver(SourceDataLine sourceDataLine) {
            this.sourceDataLine = sourceDataLine;
        }

        @Override
        public void onNext(VoiceChatResponse response) {
            n++;
            if (n % 10 == 0) {
                log.info("callId: {} : reply data size: {}", response.getCallId(), response.getData().size());
            }
            byte[] bytes = response.getData().toByteArray();
            sourceDataLine.write(bytes, 0, bytes.length);
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