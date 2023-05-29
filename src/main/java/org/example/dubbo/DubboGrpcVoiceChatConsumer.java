package org.example.dubbo;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.bootstrap.DubboBootstrap;
import org.example.dubbo.chat.DubboVoiceChatGrpc;
import org.example.dubbo.chat.VoiceChatRequest;
import org.example.dubbo.chat.VoiceChatResponse;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DubboGrpcVoiceChatConsumer {

    private static final ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(200, new NamedThreadFactory("chat-client-stream", false));

    public static void main(String[] args) throws Exception {
        DubboBootstrap bootstrap = DubboBootstrap.getInstance();
        ReferenceConfig<DubboVoiceChatGrpc.IVoiceChat> ref = new ReferenceConfig<>();
        ref.setInterface(org.example.dubbo.chat.DubboVoiceChatGrpc.IVoiceChat.class);
        ref.setProtocol("grpc");
        ref.setUrl("grpc://localhost:50051/org.example.dubbo.chat.VoiceChat");
        ApplicationConfig applicationConfig = new ApplicationConfig("grpc-stub-consumer");
        applicationConfig.setQosEnable(false);
        applicationConfig.setProtocol("grpc");
        bootstrap.application(applicationConfig).reference(ref).start();
        DubboVoiceChatGrpc.IVoiceChat voiceChat = ref.get();

        /*ClassPathXmlApplicationContext context =
                new ClassPathXmlApplicationContext("spring/dubbo-grpc-consumer.xml");
        context.start();
        DubboVoiceChatGrpc.IVoiceChat voiceChat = (DubboVoiceChatGrpc.IVoiceChat) context.getBean("voiceChat");*/

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
//            if (n % 10 == 0) {
                log.info("callId: {} : reply data size: {}", data.getCallId(), data.getData().size());
//            }
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