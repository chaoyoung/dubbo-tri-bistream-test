package com.listenrobot.dm.rpc;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.NumberUtil;
import com.alibaba.fastjson2.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.listenrobot.dm.api.constant.DMConstants;
import com.listenrobot.dm.api.constant.enums.*;
import com.listenrobot.dm.api.exception.DMException;
import com.listenrobot.dm.api.model.dto.RobotSpeech;
import com.listenrobot.dm.api.model.dto.SentenceMetadata;
import com.listenrobot.dm.api.model.dto.UserSpeech;
import com.listenrobot.dm.asr.SpeechTranscriber;
import com.listenrobot.dm.config.CallProperties;
import com.listenrobot.dm.config.NlsConfigurationProperties;
import com.listenrobot.dm.model.dialog.ChatResult;
import com.listenrobot.dm.model.dialog.DialogSession;
import com.listenrobot.dm.model.dialog.SentencePlayInfo;
import com.listenrobot.dm.service.intf.CallProcessService;
import com.listenrobot.dm.service.intf.DialogHandler;
import com.listenrobot.dm.util.ExceptionUtil;
import com.listenrobot.dm.util.SessionUtil;
import com.listenrobot.vui.common.util.VarUtils;
import com.listenrobot.vui.common.util.WaveAudioUtils;
import com.listenrobot.vui.domain.template.constant.enums.NodeType;
import com.listenrobot.vui.domain.template.core.BaseNode;
import com.listenrobot.vui.domain.template.core.NodeConfig;
import com.listenrobot.vui.domain.template.core.NodeContent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.dubbo.common.stream.StreamObserver;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.config.annotation.DubboService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Scheduled;

import javax.annotation.Resource;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * @author chengzhongchao
 * @date 2023/5/6
 * @since 5.13
 */
@DubboService(protocol = "tri", interfaceClass = VoiceChat.class, version = "1.0")
@Slf4j
public class DubboTripleVoiceChatImpl extends DubboVoiceChatTriple.VoiceChatImplBase {

    private final ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(100, new NamedThreadFactory("voice-chat-executor", true));

    @Resource
    private DialogHandler dialogHandler;

    @Resource
    private CallProcessService callProcessService;

    @Resource
    private Environment env;

    @Resource
    private ApplicationContext applicationContext;

    @Resource
    private NlsConfigurationProperties nlsConfigurationProperties;

    @Resource
    private CallProperties callProperties;

    @Value("${test.concurrent-test:false}")
    private Boolean concurrentTest;

    @Value("${test.no-user-speak:false}")
    private Boolean noUserSpeak;

    @Value("${test.no-robot-speak:false}")
    private Boolean noRobotSpeak;

    private final Path voiceRootPath = Paths.get(FileUtil.getUserHomePath() + File.separator + "voice");

    private final Counter totalCallCounter;

    /**
     * 最大通话并发数
     */
    private volatile int callMaxConcurrentCount;

    /**
     * 最大ASR连接数
     */
    private volatile int asrMaxConnections;

    public DubboTripleVoiceChatImpl(MeterRegistry registry) throws IOException {
        this.totalCallCounter = Counter.builder("rpc-voice-chat.amount").baseUnit("通").description("通话总数").register(registry);
        Gauge.builder("rpc-voice-chat.cur-concurrent-count", () -> {
            List<DialogSession> sessions = dialogHandler.getAllCallSession();
            return sessions.size();
        }).baseUnit("通").description("通话并发数").register(registry);
        Gauge.builder("asr.cur-connections", () -> {
            List<DialogSession> sessions = dialogHandler.getAllCallSession();
            return (int) sessions.stream().filter(session -> !SessionState.CLOSED.equals(session.getSessionState()))
                    .filter(DialogSession::isAsrConnected).count();
        }).baseUnit("路").description("ASR连接数").register(registry);
        Gauge.builder("rpc-voice-chat.max-concurrent-count", () -> callMaxConcurrentCount).baseUnit("通").description("通话最大并发数").register(registry);
        Gauge.builder("asr.max-connections", () -> asrMaxConnections).baseUnit("路").description("ASR最大连接数").register(registry);
        if (Files.notExists(voiceRootPath)) {
            Files.createDirectories(voiceRootPath);
        }
    }

    @Scheduled(initialDelay = 3000L, fixedRate = 5 * 1000L)
    public void reportMaxConcurrent() {
        List<DialogSession> sessions = dialogHandler.getAllCallSession();
        int callCurConcurrentCount = sessions.size();
        int asrCurConnections = (int) sessions.stream().filter(session -> !SessionState.CLOSED.equals(session.getSessionState()))
                .filter(DialogSession::isAsrConnected).count();
        if (callCurConcurrentCount > 0) {
            if (callCurConcurrentCount > callMaxConcurrentCount) {
                callMaxConcurrentCount = callCurConcurrentCount;
            }
            if (asrCurConnections > asrMaxConnections) {
                asrMaxConnections = asrCurConnections;
            }
            log.info("实时通话并发数: {}, 实时ASR连接数: {}, 当天通话最大并发数: {}, 当天ASR最大连接数: {}",
                    callCurConcurrentCount, asrCurConnections, callMaxConcurrentCount, asrMaxConnections);
        }
    }

    /**
     * 每天凌晨清空通话最大并发数和ASR最大连接数
     */
    @Scheduled(cron = "0 0 0 * * ?")
    public void clearMaxConcurrent() {
        callMaxConcurrentCount = 0;
        asrMaxConnections = 0;
    }

    @Override
    public StreamObserver<VoiceChatRequest> chat(StreamObserver<VoiceChatResponse> responseObserver) {
        log.info("接收到Triple协议请求");
        return new StreamObserver<VoiceChatRequest>() {
            int n = 0;
            String callId = "";
            DialogSession session;
            final AtomicLong lastRxTime = new AtomicLong(System.currentTimeMillis());
            final AtomicLong lastTxTime = new AtomicLong(System.currentTimeMillis());
            ScheduledFuture<?> scheduledFuture;

            @Override
            public void onNext(VoiceChatRequest request) {
                n++;
                callId = request.getCallId();
                String type = request.getType();
                byte[] data = request.getData().toByteArray();
                if (n % 10 == 0) {
                    log.debug("callId: {} : type: {}, data length: {}", callId, type, data.length);
                } else {
                    log.trace("callId: {} : type: {}, data length: {}", callId, type, data.length);
                }
                if (n == 1) {
                    session = init(callId, request.getTemplateCode(), responseObserver);
                    scheduledFuture = executorService.scheduleAtFixedRate(() -> robotTask(session, responseObserver, lastTxTime), 100L, 100L, TimeUnit.MILLISECONDS);
                    session.setScheduledFuture(scheduledFuture);
                }
                boolean isDTMF = "DTMF".equals(type);
                if (isDTMF) {
                    String dt = new String(data);
                    // DTMF编码转换
                    if ("-6".equals(dt)) {
                        dt = "*";
                    } else if ("-13".equals(dt)) {
                        dt = "#";
                    }
                    log.info("callId: {} : receive DTMF value: {}", callId, dt);
                    DialogSession session = dialogHandler.getSession(callId);
                    session.setDtReceivedTime(System.currentTimeMillis());
                    session.getDtValue().append(dt);
                } else {
                    robotListen(session, request, lastRxTime);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                log.error(String.format("callId: %s : voice chat error", callId), throwable);
                responseObserver.onError(throwable);
                doReleaseResource(callId);
            }

            @Override
            public void onCompleted() {
                log.info("callId: {} : voice chat onCompleted", callId);
                closeChat(callId, responseObserver);
            }
        };
    }

    /**
     * 拉语音流
     */
    @Override
    public void pullVoice(PullVoiceRequest request, StreamObserver<PullVoiceResponse> responseObserver) {
        log.info("pulling voice stream: {}", request);
        String callId = request.getCallId();
        String audioUrl = request.getUrl();
        if (StringUtils.isNotBlank(audioUrl)) {
            log.info("callId : {} : pulling the audio: {}", callId, audioUrl);
            byte[] audioBytes = callProcessService.getGeneralVoiceByUrl(audioUrl);
            try (ByteArrayInputStream inputStream = new ByteArrayInputStream(audioBytes)) {
                byte[] bytes = new byte[160 * 10];
                int length;
                do {
                    length = inputStream.read(bytes);
                    responseObserver.onNext(PullVoiceResponse.newBuilder()
                            .setCallId(callId).setData(ByteString.copyFrom(bytes))
                            .build());
                } while (length > 0);
                log.info("callId: {} : pulled voice stream", callId);
            } catch (IOException e) {
                log.error("callId : {} : failed pull audio {}", callId, audioUrl, e);
                responseObserver.onError(e);
            } finally {
                responseObserver.onCompleted();
            }
        }
        DialogSession session = dialogHandler.getSession(callId);
        long beginTime = System.currentTimeMillis();
        try {
            if (!session.getLock().tryLock(1000, TimeUnit.MILLISECONDS)) {
                log.error("callId: {} : failed pull voice stream, caused by failed acquire lock", callId);
                return;
            }
            int sentenceIndex = session.getSentenceIndex();
            ByteBuf sentenceBuffer = session.getSentenceBuffer();
            if (sentenceBuffer.refCnt() == 0) {
                log.warn("callId: {} : cannot pull voice stream of robot sentence {} : caused by buffer refCnt is 0", callId, sentenceIndex);
                return;
            }
            if (!sentenceBuffer.isReadable()) {
                log.warn("callId: {} : no voice stream of robot sentence {}", callId, sentenceIndex);
                return;
            }
            log.info("callId: {} : pull voice stream of robot sentence {}, length: {}", callId, sentenceIndex, sentenceBuffer.readableBytes());
            AtomicLong lastTxTime = new AtomicLong(System.currentTimeMillis());
            for (int i = 0; i < session.getSentenceTotalFrames(); i++) {
                int txIntervalMillis = (int) (System.currentTimeMillis() - lastTxTime.get());
                boolean txInterval1s = txIntervalMillis >= 1000;
                if (!sentenceBuffer.isReadable()) {
                    break;
                }
                byte[] sentenceFrameData = fetchSentenceFrameData(session, txInterval1s);
                if (sentenceFrameData == null) {
                    break;
                }
                responseObserver.onNext(PullVoiceResponse.newBuilder()
                        .setCallId(callId).setSentence(sentenceIndex).setData(ByteString.copyFrom(sentenceFrameData))
                        .build());
            }
            log.info("callId: {} : pulled voice stream", callId);
        } catch (InterruptedException e) {
            log.error("callId: {} : failed pull voice stream, caused by: {}", callId, e.getMessage());
            responseObserver.onError(e);
        } finally {
            long costTimeMillis = System.currentTimeMillis() - beginTime;
            if (costTimeMillis >= 100) {
                log.debug("callId: {} : handle dialog cost {}ms", callId, costTimeMillis);
            }
            if (session.getLock().isHeldByCurrentThread()) {
                try {
                    session.getLock().unlock();
                } catch (Exception e) {
                    log.error("callId: {} : unlock failed", callId);
                }
            }
            responseObserver.onCompleted();
        }
    }

    DialogSession init(String callId, String templateCode, StreamObserver<VoiceChatResponse> replyStream) {
        if (StringUtils.isNotEmpty(templateCode)) {
            dialogHandler.initializeDialog(callId, 0L, templateCode, Collections.emptyMap(), null, null);
        }

        // ASR并发限制判断
        /*Integer asrMaxConnection = Optional.ofNullable(nlsConfig.getAsrMaxConnection()).orElse(0);
        if (session.getUseAsr() && !session.getAsrDelayInitialize() && rSocketRequesters.size() >= asrMaxConnection) {
            throw ExceptionUtil.asrInitException(String.format("ASR并发限制（max:%d）", asrMaxConnection));
        }*/

        DialogSession session = dialogHandler.getAndSetSessionLocalCacheFromRedis(callId);
        session.setEnv(env);
        session.setApplicationContext(applicationContext);
//        session.setRSocketRequester(requester);
        session.setRobotSequenceGenerators(new AtomicInteger(0));
        session.setSentenceBuffer(PooledByteBufAllocator.DEFAULT.buffer(512 << 10, 10 << 20));
        session.setLock(new ReentrantLock());
        if (MapUtils.isEmpty(session.getDialogVarMap())) {
            session.setDialogVarMap(Maps.newHashMap());
        }
        session.setAssignVars(Lists.newArrayList());
        session.setUnrecognizedWords(Lists.newArrayList());
        session.setLabels(Maps.newLinkedHashMap());
        session.setRobotSpeechMap(Maps.newLinkedHashMap());
        session.setUserSpeeches(Lists.newLinkedList());
        session.setIntentionLevelMap(Maps.newLinkedHashMap());
        session.setNodeContentIds(Maps.newHashMap());
        session.setPositivePolicyStages(Lists.newArrayList());
        session.setNodeHitTimes(Maps.newHashMap());
        session.setSentimentHitTimes(Maps.newHashMap());
        session.setDtmfTimeoutTimes(Maps.newHashMap());
        session.setDtmfErrorTimes(Maps.newHashMap());
        session.setDialogNodeChain(Lists.newLinkedList());
        session.setIntentionPushTimes(Maps.newHashMap());
        session.setSentencePlayInfoMap(Maps.newHashMap());
        session.setTtsErrors(Lists.newArrayList());

        boolean userSpeakAfterConnectionEstablished = session.getUseAsr() && !session.getAsrDelayInitialize();
        if (!userSpeakAfterConnectionEstablished || noUserSpeak) {
            replyStream.onNext(VoiceChatResponse.newBuilder()
                    .setCallId(callId).setType(VoiceChatActionType.PAUSE_SEND_VOICE_STREAM.name())
                    .build());
        }

        try {
            if (session.getLock().tryLock(1000, TimeUnit.MILLISECONDS)) {
                ChatResult chatResult = dialogHandler.chat(callId);
                robotSpeak(session, chatResult, replyStream);
            }
        } catch (InterruptedException e) {
            throw ExceptionUtil.chatException(callId, "call setup failed: " + e.getMessage());
        } finally {
            if (session.getLock().isHeldByCurrentThread()) {
                try {
                    session.getLock().unlock();
                } catch (Exception e) {
                    log.error("callId: {} : unlock failed", callId);
                }
            }
        }

        if (userSpeakAfterConnectionEstablished && !noUserSpeak) {
            session.ensureAsrConnectionAvailable();
        }
        boolean enableGenderIdentification = Optional.ofNullable(session.getEnableGenderIdentification()).orElse(false);
        if (enableGenderIdentification && !"default".equals(nlsConfigurationProperties.getConfig().getToken())) {
            if (StringUtils.isNotBlank(nlsConfigurationProperties.getConfig().getGenderIdentificationAppKey())) {
                session.ensureGenderIdentificationAvailable();
            } else {
                log.warn("callId: {} : cannot enable gender identification caused by app-key is missing", callId);
            }
        }
        log.info("callId: {} : call connection established", callId);
        callProcessService.initializeAllTtsVariables(session);
        totalCallCounter.increment();
        return session;
    }

    void robotListen(DialogSession session, VoiceChatRequest request, AtomicLong lastRxTime) {
        String callId = session.getCallId();
        byte[] bytes = request.getData().toByteArray();
        int dataSize = ArrayUtils.getLength(bytes);
        if (dataSize > 0) {
            session.setUserVoiceReceived(true);
        }
        long currentTimeMillis = System.currentTimeMillis();
        int rxIntervalMillis = (int) (currentTimeMillis - lastRxTime.get());
        boolean rxInterval1s = rxIntervalMillis >= 1000;
        if (rxIntervalMillis >= 2000) {
            log.warn("callId: {} : rx: too large binary interval: {} million seconds, {} bytes", callId, rxIntervalMillis, dataSize);
            lastRxTime.set(currentTimeMillis);
        } else if (rxInterval1s) {
            log.debug("callId: {} : rx: interval: {} million seconds, {} bytes", callId, rxIntervalMillis, dataSize);
            lastRxTime.set(currentTimeMillis);
        }
        if (SessionState.CLOSED.equals(session.getSessionState())) {
            if (rxInterval1s) {
                log.info("callId: {} : session has been closed", callId);
            }
            return;
        }
        if (!session.isAsrConnected()) {
            if (rxInterval1s) {
                log.debug("callId: {} : asr not connected", callId);
            }
            return;
        }
        if (session.getSpeechTranscriber() == null) {
            if (rxInterval1s) {
                log.info("callId: {} : no asr connection", callId);
            }
            return;
        }
        SpeechTranscriber speechTranscriber = session.getSpeechTranscriber();
        try {
            speechTranscriber.send(bytes);
        } catch (Exception e) {
            log.warn("callId: {} : failed send data to asr, taskId: {}, retry now.", callId, speechTranscriber.getTaskId(), e);
            speechTranscriber.reconnect();
            speechTranscriber.send(bytes);
        }
        boolean enableGenderIdentification = Optional.ofNullable(session.getEnableGenderIdentification()).orElse(false);
        if (enableGenderIdentification && session.getGenderIdentificationRequester() != null && session.getGenderIdentificationRequester().isRunning()) {
            boolean robotTimeLt60s = (int) (System.currentTimeMillis() - session.getConnectTime()) / 1000 < 60;
            if (robotTimeLt60s) {
                try {
                    session.getGenderIdentificationRequester().send(bytes);
                } catch (Exception e) {
                    log.error("callId: {} : failed send data to nls gender identification server, taskId: {}, state: {}",
                            callId, session.getGenderIdentificationRequester().getTaskId(), session.getGenderIdentificationRequester().getState(), e);
                    session.closeGenderIdentification();
                    session.ensureGenderIdentificationAvailable();
                    session.getGenderIdentificationRequester().send(bytes);
                }
            } else {
                session.stopGenderIdentification();
            }
        }
    }

    void robotTask(DialogSession session, StreamObserver<VoiceChatResponse> responseObserver, AtomicLong lastTxTime) {
        String callId = session.getCallId();
        int txIntervalMillis = (int) (System.currentTimeMillis() - lastTxTime.get());
        boolean txInterval1s = txIntervalMillis >= 1000;
        if (txInterval1s) {
            lastTxTime.set(System.currentTimeMillis());
        }
        long beginTime = System.currentTimeMillis();
        try {
            if (!session.getLock().tryLock()) {
                return;
            }
            DMException asrException = Optional.ofNullable(session.getSpeechTranscriber())
                    .map(SpeechTranscriber::getException)
                    .orElse(null);
            if (asrException != null) {
                if (asrException.getCode() == DMErrorCode.USER_VOICE_IDLE_TIMEOUT.code && session.isUserVoiceReceived()) {
                    log.warn(asrException.getMessage() + " : " + asrException.getDetails());
                    closeChat(callId, responseObserver);
                    return;
                }
                throw asrException;
            }
            SessionState sessionState = session.getSessionState();
            int robotTime = (int) (System.currentTimeMillis() - session.getConnectTime());
            long robotIdleTimeMillis = System.currentTimeMillis() - Optional.ofNullable(session.getRobotPlayTime())
                    .orElse(session.getConnectTime());
            if (SessionState.isUserInteractive(sessionState) || SessionState.isClosedOrCloseWait(sessionState)) {
                if (!session.isUserSpeaking() && robotIdleTimeMillis >= DMConstants.ROBOT_IDLE_TIMEOUT_MILLIS) {
                    log.warn("callId: {} : 机器人空闲超时（{} milliseconds）", callId, robotIdleTimeMillis);
                    closeChat(callId, responseObserver);
                    return;
                }
                if (SessionState.isClosed(sessionState)) {
                    return;
                }
                if (SessionState.isUserInteractive(sessionState)) {
                    // 机器人进入静默状态
                    if (session.isSilence()) {
                        session.setSilence(false);
                        session.setSessionState(SessionState.SILENCE);
                    }
                    // AI分流就绪
                    if (Optional.ofNullable(session.getRobotBypassReady()).orElse(false)) {
                        session.setRobotBypassReady(null);
                        session.ensureAsrConnectionAvailable();
                        responseObserver.onNext(VoiceChatResponse.newBuilder()
                                .setCallId(callId).setType(VoiceChatActionType.RESUME_SEND_VOICE_STREAM.name())
                                .build());
                        log.info("callId: {} : notify resume send voice stream", callId);
                    }
                }
            }
            int callDurationMillis = (int) (System.currentTimeMillis() - session.getConnectTime());
            int callDuration = NumberUtil.ceilDiv(callDurationMillis, 1000);
            session.setCallDuration(callDuration);
            if (callDuration >= session.getMaxCallDuration()) {
                log.warn("callId: {} : 对话超时（{}s/{}s）", callId, callDuration, session.getMaxCallDuration());
                closeChat(callId, responseObserver);
                return;
            }
            if (session.getRobotSpeechTimes() >= session.getMaxInteractiveRounds()) {
                log.warn("callId: {} : 对话轮数超限（{}/{}）", callId, session.getRobotSpeechTimes(), session.getMaxInteractiveRounds());
                closeChat(callId, responseObserver);
                return;
            }
            switch (sessionState) {
                case CHATTING: {
                    if (session.getSilenceTransferAgent() != null && session.isCcTransferAgentReady()) {
                        session.setCcTransferAgentReady(false);
                        session.setSessionState(SessionState.WAIT_TRANSFER_AGENT);
                    }
                    UserSpeech userSpeech = session.poolUserSpeech();
                    if (userSpeech != null) {
                        if (session.isSilence()) {
                            session.setSessionState(SessionState.SILENCE);
                            return;
                        }
                        ChatResult chatResult = dialogHandler.chat(callId, userSpeech, robotTime);
                        robotSpeak(session, chatResult, responseObserver);
                    }
                    break;
                }
                case DTMF_CONFIRM: {
                    boolean receiveDTValue = StringUtils.isNotBlank(session.getDtValue());
                    if (receiveDTValue) {
                        session.clearSpeechTranscriberQueues("DTMF confirm");
                        ChatResult chatResult = dialogHandler.dtmfConfirm(callId, session.getDtValue().toString(), robotTime);
                        robotSpeak(session, chatResult, responseObserver);
                    }
                    if (session.isRobotPlaying() && !session.isCurrNodeInterruptEnabled()) {
                        session.clearSpeechTranscriberQueues("curr node interrupt disabled");
                    } else if (session.isDtmfConfirmVoiceRecognitionEnabled()) {
                        Long receiveTime = receiveDTValue
                                ? Optional.ofNullable(session.getDtReceivedTime()).orElse(System.currentTimeMillis())
                                : Optional.ofNullable(session.peekUserSpeech()).map(UserSpeech::getReceiveTime)
                                .map(time -> time.atZone(ZoneId.systemDefault()).toEpochSecond())
                                .orElse(System.currentTimeMillis());
                        long receiveWaitIdleTime = System.currentTimeMillis() - receiveTime;
                        if (receiveWaitIdleTime >= 1000) {
                            UserSpeech userSpeech = session.poolUserSpeech();
                            if (userSpeech != null) {
                                ChatResult chatResult = dialogHandler.chat(callId, userSpeech, robotTime);
                                robotSpeak(session, chatResult, responseObserver);
                            }
                        }
                    }
                    break;
                }
                case SILENCE: {
                    if (txInterval1s) {
                        log.info("callId: {} : robot silence", callId);
                    }
                    session.clearSpeechTranscriberQueues("session state : SILENCE");
                    return;
                }
            }
            if (session.isRobotPlaying() && RobotControlPlayStatus.PAUSE.equals(session.getRobotControlPlayStatus())) {
                session.setRobotPlaying(false);
                session.setRobotControlPlayStatus(RobotControlPlayStatus.DEFAULT);
                responseObserver.onNext(VoiceChatResponse.newBuilder()
                        .setCallId(callId).setType(VoiceChatActionType.PAUSE_PLAY.name())
                        .build());
                log.info("callId: {} : pause play", callId);
            } else if (!session.isRobotPlaying() && RobotControlPlayStatus.PLAY.equals(session.getRobotControlPlayStatus())) {
                session.setRobotPlaying(true);
                session.setRobotControlPlayStatus(RobotControlPlayStatus.DEFAULT);
                responseObserver.onNext(VoiceChatResponse.newBuilder()
                        .setCallId(callId).setType(VoiceChatActionType.RESUME_PLAY.name())
                        .build());
                log.info("callId: {} : resume play", callId);
            }
            if (session.isRobotPlayEnd()) {
                switch (sessionState) {
                    case CHATTING: {
                        ChatResult chatResult = dialogHandler.timeout(session, robotIdleTimeMillis, robotTime);
                        robotSpeak(session, chatResult, responseObserver);
                        break;
                    }
                    case WAIT_TRANSFER_AGENT: {
                        dialogHandler.transferAgent(callId);
                        if (session.isAsrConnected()) {
                            try {
                                log.info("callId: {} : closing asr connection", callId);
                                responseObserver.onNext(VoiceChatResponse.newBuilder()
                                        .setCallId(callId).setType(VoiceChatActionType.PAUSE_SEND_VOICE_STREAM.name())
                                        .build());
                                log.info("callId: {} : notify pause send voice stream", callId);
                                session.closeAsr();
                            } catch (Exception e) {
                                log.error("callId: {} : failed close asr connection", callId, e);
                            }
                        }
                        break;
                    }
                    case TRANSFER_AGENT_WAIT: {
                        Boolean transferAgentCallback = session.getCcTransferAgentResult();
                        if (transferAgentCallback != null) {
                            session.setCcTransferAgentResult(null);
                            if (transferAgentCallback) {
                                session.setSessionState(SessionState.TRANSFER_AGENT_SUCCEED);
                            } else {
                                session.setSessionState(SessionState.TRANSFER_AGENT_FAILED);
                            }
                            break;
                        }
                        if (robotIdleTimeMillis >= session.getTransferAgentWaitTimeoutMillis()) {
                            log.warn("callId: {} : 转人工等待超时（{} milliseconds）", callId, robotIdleTimeMillis);
                            session.setSessionState(SessionState.TRANSFER_AGENT_FAILED);
                        } else if (txInterval1s) {
                            log.info("callId: {} : transfer agent wait", callId);
                        }
                        break;
                    }
                    case TRANSFER_AGENT_SUCCEED: {
                        closeChat(callId, responseObserver);
                        break;
                    }
                    case TRANSFER_AGENT_FAILED: {
                        ChatResult chatResult = dialogHandler.transferAgentFailed(callId, robotTime);
                        boolean shutdown = Optional.ofNullable(chatResult.getRobotSpeech())
                                .map(RobotSpeech::getActionType).map(ActionType.SHUTDOWN::equals)
                                .orElse(false);
                        if (!shutdown) {
                            session.ensureAsrConnectionAvailable();
                            responseObserver.onNext(VoiceChatResponse.newBuilder()
                                    .setCallId(callId).setType(VoiceChatActionType.RESUME_SEND_VOICE_STREAM.name())
                                    .build());
                            log.info("callId: {} : notify resume send voice stream", callId);
                        }
                        robotSpeak(session, chatResult, responseObserver);
                        break;
                    }
                    case DTMF_CONFIRM: {
                        if (session.getDtReceivedTime() == null) {
                            session.setDtReceivedTime(System.currentTimeMillis());
                        }
                        long dtmfConfirmCostTime = System.currentTimeMillis() - session.getDtReceivedTime();
                        if (dtmfConfirmCostTime >= session.getDtmfTimeoutDuration()) {
                            ChatResult chatResult = dialogHandler.dtmfTimeout(session, robotTime);
                            robotSpeak(session, chatResult, responseObserver);
                        }
                        break;
                    }
                    case LEAVE_MESSAGE: {
                        session.ensureAsrConnectionAvailable();
                        responseObserver.onNext(VoiceChatResponse.newBuilder()
                                .setCallId(callId).setType(VoiceChatActionType.RESUME_SEND_VOICE_STREAM.name())
                                .build());
                        log.info("callId: {} : notify resume send voice stream", callId);
                        Optional.ofNullable(session.getSpeechTranscriber())
                                .map(SpeechTranscriber::pollText)
                                .ifPresent(userSpeech -> {
                                    log.info("callId: {} : record {}th user leave message: {}", callId, userSpeech.getSequence(), userSpeech.getText());
                                    userSpeech.setLeaveMessage(true);
                                    session.getUserSpeeches().add(userSpeech);
                                });
                        if (session.getLeaveMessageStartTime() == null) {
                            session.setLeaveMessageStartTime(System.currentTimeMillis());
                        }
                        long leaveMessageTimeDuration = System.currentTimeMillis() - session.getLeaveMessageStartTime();
                        Integer leaveMessageTimeout = Optional.ofNullable(session.getLeaveMessageTimeout())
                                .orElse(DMConstants.DEFAULT_LEAVE_MESSAGE_TIMEOUT_MILLIS);
                        // 留言超时
                        if (leaveMessageTimeDuration >= leaveMessageTimeout) {
                            closeChat(callId, responseObserver);
                        }
                        break;
                    }
                    case CLOSE_WAIT: {
                        closeChat(callId, responseObserver);
                        break;
                    }
                }
            } else {
                SentencePlayInfo sentencePlayInfo = session.getSentencePlayInfoMap().get(session.getSentenceIndex());
                if (sentencePlayInfo != null && RobotControlPlayStatus.DEFAULT.equals(session.getRobotControlPlayStatus())) {
                    switch (sentencePlayInfo.getSentencePlayStatus()) {
                        case NONE: {
                            long sentencePlayRemainingTime = session.getSentenceTotalTime() - session.getSentencePlayTimeDuration();
                            log.info("callId: {} : {}th sentence play begin, sentenceTotalTime: {}, sentencePlayTimeDuration: {}, sentencePlayRemainingTime: {}",
                                    callId, session.getSentenceIndex(), session.getSentenceTotalTime(), session.getSentencePlayTimeDuration(), sentencePlayRemainingTime);
                            sentencePlayInfo.setSentencePlayStatus(SentencePlayStatus.PLAY).setStartPlayTime(System.currentTimeMillis());
                            session.setRobotPlaying(true);
                            session.setSentencePlayTimeDuration(0);
                            session.setRobotPlayTime(System.currentTimeMillis());
                            session.setRobotSentenceBeginTime(System.currentTimeMillis());
                        }
                        case PLAY: {
                            int delay = (int) (System.currentTimeMillis() - Optional.ofNullable(session.getRobotPlayTime())
                                    .orElse(System.currentTimeMillis()));
                            session.setRobotPlaying(true);
                            session.setSentencePlayTimeDuration(session.getSentencePlayTimeDuration() + delay);
                            session.setRobotPlayTime(System.currentTimeMillis());
                            long sentencePlayRemainingTime = session.getSentenceTotalTime() - session.getSentencePlayTimeDuration();
                            boolean sentencePlayEnd = SessionState.CLOSE_WAIT.equals(sessionState) ? sentencePlayRemainingTime <= -800 : sentencePlayRemainingTime <= 200;
                            if (sentencePlayEnd) {
                                log.info("callId: {} : {}th sentence play end, sentenceTotalTime: {}, sentencePlayTimeDuration: {}, sentencePlayRemainingTime: {}",
                                        callId, session.getSentenceIndex(), session.getSentenceTotalTime(), session.getSentencePlayTimeDuration(), sentencePlayRemainingTime);
                                sentencePlayInfo.setSentencePlayStatus(SentencePlayStatus.ENDED).setEndPlayTime(System.currentTimeMillis());
                                session.setRobotPlaying(false);
                                session.setRobotPlayEnd(true);
                                session.setRobotSentenceEndTime(System.currentTimeMillis());
                            } else if (txInterval1s) {
                                log.debug("callId: {} : {}th sentence playing, sentenceTotalTime: {}, sentencePlayTimeDuration: {}, sentencePlayRemainingTime: {}",
                                        callId, session.getSentenceIndex(), session.getSentenceTotalTime(), session.getSentencePlayTimeDuration(), sentencePlayRemainingTime);
                            }
                        }
                    }
                }
            }
        } finally {
            long costTimeMillis = System.currentTimeMillis() - beginTime;
            if (txInterval1s && costTimeMillis >= 100) {
                log.debug("callId: {} : handle dialog cost {}ms", callId, costTimeMillis);
            }
            if (session.getLock().isHeldByCurrentThread()) {
                try {
                    session.getLock().unlock();
                } catch (Exception e) {
                    log.error("callId: {} : unlock failed", callId);
                }
            }
        }
    }

    private void robotSpeak(DialogSession session, ChatResult chatResult, StreamObserver<VoiceChatResponse> responseObserver) {
        String callId = session.getCallId();
        RobotSpeech robotSpeech = chatResult.getRobotSpeech();
        if (robotSpeech == null) {
            return;
        }
        if (ActionType.IGNORE.equals(robotSpeech.getActionType())) {
            long sentencePlayRemainingTime = session.getSentenceTotalTime() - session.getSentencePlayTimeDuration();
            if (session.isEnableVoiceInterrupt() && !session.isRobotPlaying() && sentencePlayRemainingTime > 0) {
                session.setRobotControlPlayStatus(RobotControlPlayStatus.PLAY);
            }
            return;
        }
        List<NodeContent> nodeContents = robotSpeech.getNodeContents();
        if (CollectionUtils.isEmpty(nodeContents)) {
            return;
        }
        Integer interruptTimeThreshold = Optional.ofNullable(robotSpeech.getInterruptTimeThreshold()).orElse(30 * 1000);
        Integer sentenceIndex = robotSpeech.getSequence();
        boolean interrupt = Optional.ofNullable(robotSpeech.getInterrupt()).orElse(false);
        ByteBuf sentenceBuffer = session.getSentenceBuffer();
        if (sentenceBuffer.refCnt() == 0) {
            log.warn("callId: {} : ignore robot sentence {}, caused by ByteBuf has been release", callId, sentenceIndex);
            return;
        }
        List<String> audioUrls = nodeContents.stream().map(NodeContent::getUrl).collect(Collectors.toList());
        byte[] pcmBytes;
        try {
            pcmBytes = callProcessService.replacePlaceholderWithTtsVoice(callId, audioUrls, chatResult.getTtsSettings(), true);
        } catch (Exception e) {
            log.error("callId: {} : failed parse sentence voice", callId, e);
            return;
        }
        if (ArrayUtils.isEmpty(pcmBytes)) {
            log.warn("callId: {} : resolve robot voice is empty, audioUrls: {}", callId, audioUrls);
            return;
        }
        boolean urlContainsVar = audioUrls.stream().anyMatch(url -> VarUtils.containsComplexVar.test(url));
        if (concurrentTest && urlContainsVar) {
            try {
                Path voicePath = Paths.get(voiceRootPath + File.separator + callId + "_" + sentenceIndex + ".wav");
                log.info("callId: {} : writing voice bytes to file: {}", callId, voicePath);
                byte[] wavBytes = WaveAudioUtils.pcm2wav(pcmBytes, DMConstants.SAMPLE_RATE);
                Files.write(voicePath, wavBytes);
            } catch (IOException e) {
                log.error("callId: {} : failed write voice bytes to file", callId, e);
            }
        }
        int availableDataSize = sentenceBuffer.readableBytes();
        if (!interrupt) {
            int sleepDelta = WaveAudioUtils.eval16BitMonoVoiceTimeLength(availableDataSize, DMConstants.SAMPLE_RATE);
            if (availableDataSize > 0 && sleepDelta <= interruptTimeThreshold) {
                log.info("callId: {} : delay interrupt, threshold：{} ms, time remain：{}ms, data size remain：{} byte",
                        callId, interruptTimeThreshold, sleepDelta, availableDataSize);
                interrupt = true;
            } else if (availableDataSize > 0) {
                log.info("callId: {} : data remain interrupt, data size remain {} byte", callId, availableDataSize);
                interrupt = true;
            }
        }
        if (!interrupt && !session.isRobotPlayEnd()) {
            interrupt = true;
        }
        if (session.isRobotSpeechEnd()) {
            interrupt = true;
        }
        if (interrupt) {
            sentenceBuffer.clear();
        }
        session.setSentenceIndex(sentenceIndex).setRobotPlayEnd(false);
        int sentenceLength = pcmBytes.length;
        session.setSentenceLength(sentenceLength);
        int sentenceTotalTime = WaveAudioUtils.eval16BitMonoVoiceTimeLength(sentenceLength, DMConstants.SAMPLE_RATE);
        session.setSentenceInterrupt(interrupt);
        BaseNode flowNode = SessionUtil.getFlowNode(session);
        boolean interruptEnabled = Optional.ofNullable(flowNode.getNodeConfig()).map(NodeConfig::getInterruptEnabled)
                .orElse(false);
        session.setCurrNodeInterruptEnabled(interruptEnabled);
        boolean isDTMFNode = Optional.ofNullable(flowNode.getNodeType()).map(NodeType::isDTMFNode).orElse(false);
        if (isDTMFNode) {
            boolean enableVoiceRecognition = Optional.ofNullable(flowNode.getNodeConfig())
                    .map(NodeConfig::getEnableVoiceRecognition).orElse(false);
            session.setDtmfConfirmVoiceRecognitionEnabled(enableVoiceRecognition);
        } else {
            session.setDtmfConfirmVoiceRecognitionEnabled(false);
        }
        session.setSentenceTotalTime(sentenceTotalTime);
        session.setSentencePlayTimeDuration(0);
        session.getSentenceCurrentFrame().set(0);
        int sentenceTotalFrames = (int) Math.ceil((double) sentenceLength / session.getFrameLength());
        session.setSentenceTotalFrames(sentenceTotalFrames);
        session.setLastSendTime(System.currentTimeMillis());
        session.setDtReceivedTime(null);
        session.setRobotSentenceBeginTime(null);
        session.setRobotSentenceEndTime(null);
        sentenceBuffer.writeBytes(pcmBytes);
        session.getSentencePlayInfoMap().put(sentenceIndex, new SentencePlayInfo().setSentencePlayStatus(SentencePlayStatus.NONE));
        if (noRobotSpeak) {
            sentenceBuffer.clear();
            return;
        }
        if (callProperties.getSendVoiceByRequestChannel()) {
            if (interrupt) {
                responseObserver.onNext(VoiceChatResponse.newBuilder()
                        .setCallId(callId).setType(VoiceChatActionType.CLEAR_VOICE_STREAM.name())
                        .build());
                log.info("callId: {} : notify clear voice stream", callId);
            }
        } else {
            SentenceMetadata sentenceMetadata = new SentenceMetadata()
                    .setCallId(callId).setInterrupt(interrupt).setSentenceIndex(sentenceIndex).setSentenceLength(sentenceLength)
                    .setSentenceTotalTime(sentenceTotalTime).setSentenceTotalFrames(sentenceTotalFrames)
                    .setIsLastSentence(session.isRobotSpeechEnd())
                    .setContainsVar(urlContainsVar);
            if (concurrentTest && !urlContainsVar) {
                sentenceMetadata.setAudioUrls(audioUrls);
            }
            responseObserver.onNext(VoiceChatResponse.newBuilder()
                    .setCallId(callId)
                    .setType(VoiceChatActionType.PULL_VOICE_STREAM.name())
                    .setData(JSON.toJSONString(sentenceMetadata))
                    .build());
            log.info("callId: {} : notify pull voice stream", callId);
        }
        if (RobotControlPlayStatus.PAUSE.equals(session.getRobotControlPlayStatus())) {
            responseObserver.onNext(VoiceChatResponse.newBuilder()
                    .setCallId(callId).setType(VoiceChatActionType.RESUME_PLAY.name())
                    .build());
            log.info("callId: {} : notify resume play", callId);
        }
        callProcessService.pushIntention(callId, robotSpeech.getIntentionPushEnabled(), chatResult.getIntentionLevel());
    }

    private byte[] fetchSentenceFrameData(DialogSession session, boolean txInterval1s) {
        String callId = session.getCallId();
        long currentTimeMillis = System.currentTimeMillis();
        ByteBuf sentenceBuffer = session.getSentenceBuffer();
        if (!sentenceBuffer.isReadable()) {
            return null;
        }
        if (noRobotSpeak) {
            sentenceBuffer.clear();
            return null;
        }
        boolean sentenceInterrupt = session.isSentenceInterrupt();
        if (sentenceInterrupt) {
            session.setSentenceInterrupt(false);
        }
        int readableSentenceLength = sentenceBuffer.readableBytes();
        int currentFrameLength = Math.min(readableSentenceLength, session.getFrameLength());
        byte[] currentFrameBytes = new byte[currentFrameLength];
        sentenceBuffer.readBytes(currentFrameBytes);
        boolean isSentenceLastFrame = !sentenceBuffer.isReadable();
        int sentenceIndex = session.getSentenceIndex();
        int sentenceLength = session.getSentenceLength();
        int sentenceCurrentFrame = session.getSentenceCurrentFrame().incrementAndGet();
        int sentenceTotalFrames = session.getSentenceTotalFrames();
        int callTotalFrames = session.getCallTotalFrames().incrementAndGet();
        int txInterval = (int) (currentTimeMillis - session.getLastSendTime());
        session.setLastSendTime(currentTimeMillis);
        boolean isSentenceFirstFrame = !isSentenceLastFrame && sentenceLength > 0;
        if (isSentenceFirstFrame) {
            // 每句话首帧
            session.setSentenceLength(0);
            log.info("callId: {}, send sentence first frame at {}ms intervals. callTotalFrames:{}, sentenceIndex: {}, sentenceCurrentFrame: {}, currentFrameLength: {}, sentenceTotalFrames: {}, sentenceLength: {}, sentenceInterrupt: {}",
                    callId, txInterval, callTotalFrames, sentenceIndex, sentenceCurrentFrame, currentFrameLength, sentenceTotalFrames, sentenceLength / 1024, sentenceInterrupt);
        } else if (isSentenceLastFrame) {
            // 每句话最后一帧
            log.info("callId: {}, send sentence last frame at {}ms intervals. callTotalFrames:{}, sentenceIndex: {}, sentenceCurrentFrame: {}, currentFrameLength: {}, sentenceTotalFrames: {}",
                    callId, txInterval, callTotalFrames, sentenceIndex, sentenceCurrentFrame, currentFrameLength, sentenceTotalFrames);
            // 回收已读字节空间
            sentenceBuffer.discardReadBytes();
        } else if (txInterval1s) {
            // 每句话中间帧
            log.debug("callId: {}, send sentence middle frame at {}ms intervals. callTotalFrames:{}, sentenceIndex: {}, sentenceCurrentFrame: {}, currentFrameLength: {}, sentenceTotalFrames: {}",
                    callId, txInterval, callTotalFrames, sentenceIndex, sentenceCurrentFrame, currentFrameLength, sentenceTotalFrames);
        }
        return currentFrameBytes;
    }

    public void closeChat(String callId, StreamObserver<VoiceChatResponse> responseObserver) {
        doReleaseResource(callId);
        responseObserver.onCompleted();
    }

    /**
     * 释放资源
     */
    public void doReleaseResource(String callId) {
        log.info("callId: {} : do release resource", callId);
        DialogSession session = null;
        try {
            session = dialogHandler.getSession(callId);
            log.info("callId: {} : releasing call resource : current SessionState={}", callId, session.getSessionState());
            ScheduledFuture<?> scheduledFuture = session.getScheduledFuture();
            if (scheduledFuture != null && !scheduledFuture.isCancelled()) {
                try {
                    scheduledFuture.cancel(false);
                } catch (Exception e) {
                    log.error(String.format("callId: %s : failed cancel thread scheduling", callId), e);
                }
            }
            ByteBuf sentenceBuffer = session.getSentenceBuffer();
            if (sentenceBuffer != null && sentenceBuffer.refCnt() > 0) {
                try {
                    ReferenceCountUtil.release(sentenceBuffer);
                    log.info("callId: {} : sentence buffer released", callId);
                } catch (Throwable e) {
                    log.error("callId: {} : failed release sentence buffer {}", callId, sentenceBuffer, e);
                }
            }
            if (SessionState.CLOSED.equals(session.getSessionState())) {
                return;
            }
            session.setSessionState(SessionState.CLOSED);
            log.info("callId: {} : robot speech end: {}, play end: {}", callId, session.isRobotSpeechEnd(), session.isRobotPlayEnd());
            session.closeAsr();
            session.closeGenderIdentification();
            dialogHandler.close(callId);
        } catch (DMException e) {
            if (StringUtils.contains(e.getMessage(), DMConstants.NO_DIALOG_SESSION)) {
                log.info("callId: {} : cannot release resource: {}", callId, DMConstants.NO_DIALOG_SESSION);
                return;
            }
            log.error("callId: {} : failed release resource", callId, e);
        } finally {
            try {
                Optional.ofNullable(session).map(DialogSession::getLock).ifPresent(Lock::unlock);
            } catch (Exception ignore) {
            }
        }
    }

}
