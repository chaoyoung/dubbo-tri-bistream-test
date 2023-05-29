package org.example.voicechat.client.rpc;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

@Slf4j
public class ReactorDubboGrpcVoiceChatImpl extends ReactorDubboVoiceChatGrpc.VoiceChatImplBase {

    @Override
    public Flux<VoiceChatReply> chat(Flux<VoiceChatRequest> request) {
        return request.map(req -> VoiceChatReply.newBuilder().setData(req.getData()).build());
    }
}
