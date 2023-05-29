package org.example.dubbo.chat;

import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.apache.dubbo.common.stream.StreamObserver;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.config.annotation.DubboService;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author chaoyoung
 * @date 2023/5/22
 * @since
 */
//@DubboService(protocol = "tri", interfaceClass = VoiceChat.class, path = "org.example.dubbo.chat.VoiceChat")
@Slf4j
public class DubboTripleVoiceChatImpl extends DubboVoiceChatTriple.VoiceChatImplBase {

    private final ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(200, new NamedThreadFactory("voice-chat-executor", true));

    @Override
    public StreamObserver<VoiceChatRequest> chat(StreamObserver<VoiceChatResponse> responseObserver) {
        log.info("接收到Triple协议请求");
        return new StreamObserver<VoiceChatRequest>() {
            int n = 0;
            String callId = "";
            ScheduledFuture<?> scheduledFuture;

            @Override
            public void onNext(VoiceChatRequest request) {
                n++;
                callId = request.getCallId();
                byte[] data = request.getData().toByteArray();
                if (n % 10 == 0) {
                    log.info("callId: {} : data size: {}", callId, data.length);
                }
                if (n == 1) {
                    scheduledFuture = executorService.scheduleAtFixedRate(() -> {
                        responseObserver.onNext(VoiceChatResponse.newBuilder()
                                .setCallId(callId).setData(ByteString.copyFrom(new byte[160]))
                                .build());
                    }, 100L, 100L, TimeUnit.MILLISECONDS);
                }
                if (n == 10000000) {
                    log.info("server on completed");
                    scheduledFuture.cancel(false);
                    responseObserver.onCompleted();
                }
            }

            @Override
            public void onError(Throwable throwable) {
                log.error(String.format("callId: %s : voice chat error", callId), throwable);
                responseObserver.onError(throwable);
            }

            @Override
            public void onCompleted() {
                log.info("callId: {} : voice chat onCompleted", callId);
                responseObserver.onCompleted();
            }
        };
    }

}
