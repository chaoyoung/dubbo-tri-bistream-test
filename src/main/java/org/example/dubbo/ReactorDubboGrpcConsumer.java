package org.example.dubbo;

import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.bootstrap.DubboBootstrap;
import org.example.dubbo.chat.ReactorDubboVoiceChatGrpc;
import org.example.dubbo.chat.VoiceChatRequest;
import reactor.core.publisher.Flux;

import java.time.Duration;

@Slf4j
public class ReactorDubboGrpcConsumer {

    public static void main(String[] args) throws Exception {
        ApplicationConfig applicationConfig = new ApplicationConfig("reactor-dubbo-grpc-stub-consumer");
        applicationConfig.setQosEnable(false);
        ProtocolConfig protocolConfig = new ProtocolConfig("grpc");
        protocolConfig.setSerialization("protobuf");

        ReferenceConfig<ReactorDubboVoiceChatGrpc.IReactorVoiceChat> ref = new ReferenceConfig<>();
        ref.setInterface(ReactorDubboVoiceChatGrpc.IReactorVoiceChat.class);
//        ref.setProtocol("grpc");
//        ref.setUrl("grpc://localhost:50051/org.example.dubbo.chat.VoiceChat");
        ref.setTimeout(10000);

        DubboBootstrap bootstrap = DubboBootstrap.getInstance();
        bootstrap.application(applicationConfig)
                .registry(new RegistryConfig("zookeeper://localhost:2181"))
                .protocol(protocolConfig)
                .reference(ref)
                .start();
        ReactorDubboVoiceChatGrpc.IReactorVoiceChat voiceChat = ref.get();

        voiceChat
                .chat(Flux.range(1, 10)
                        .map(num -> VoiceChatRequest.newBuilder().setCallId("123456").setData(ByteString.copyFrom(new byte[160])).build())
                ).subscribe(response -> log.info("Reply data size: {}", response.getData().size()));

        System.in.read();
    }
}
