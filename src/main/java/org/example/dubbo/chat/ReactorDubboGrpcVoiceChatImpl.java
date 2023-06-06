package org.example.dubbo.chat;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

@Slf4j
public class ReactorDubboGrpcVoiceChatImpl extends ReactorDubboVoiceChatGrpc.VoiceChatImplBase {

    @Override
    public Flux<VoiceChatResponse> chat(Flux<VoiceChatRequest> request) {
        return request.map(req -> VoiceChatResponse.newBuilder().setData(req.getData()).build());
    }
}
