package org.example.dubbo;

import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.ServiceConfig;
import org.apache.dubbo.config.bootstrap.DubboBootstrap;
import org.example.dubbo.chat.ReactorDubboGrpcVoiceChatImpl;
import org.example.dubbo.chat.ReactorDubboVoiceChatGrpc;
import org.example.dubbo.util.EmbeddedZooKeeper;

import java.util.concurrent.CountDownLatch;

public class ReactorDubboGrpcProvider {

    public static void main(String[] args) throws Exception {
        new EmbeddedZooKeeper(2181, true).start();

        ApplicationConfig applicationConfig = new ApplicationConfig("reactor-dubbo-grpc-stub-server");
        applicationConfig.setProtocol("grpc");
        applicationConfig.setQosEnable(false);
        ProtocolConfig protocolConfig = new ProtocolConfig("grpc", 50051);
        protocolConfig.setSerialization("protobuf");

        ServiceConfig<ReactorDubboVoiceChatGrpc.IReactorVoiceChat> service = new ServiceConfig<>();
        service.setInterface(ReactorDubboVoiceChatGrpc.IReactorVoiceChat.class);
        service.setRef(new ReactorDubboGrpcVoiceChatImpl());
        service.setPath("org.example.dubbo.chat.VoiceChat");

        DubboBootstrap bootstrap = DubboBootstrap.getInstance();
        bootstrap.application(applicationConfig)
                .registry(new RegistryConfig("zookeeper://localhost:2181"))
                .protocol(protocolConfig)
                .service(service)
                .start();

        System.out.println("dubbo service started");
        new CountDownLatch(1).await();
    }
}
