package com.tvd12.ezymq.activemq.manager;

import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezymq.activemq.EzyActiveRpcProducer;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveRpcClient;
import com.tvd12.ezymq.activemq.setting.EzyActiveRpcProducerSetting;

import javax.jms.ConnectionFactory;
import javax.jms.Session;
import java.util.HashMap;
import java.util.Map;

public class EzyActiveRpcProducerManager extends EzyActiveAbstractManager {

    protected final EzyEntityCodec entityCodec;
    protected final Map<String, EzyActiveRpcProducer> rpcProducers;
    protected final Map<String, EzyActiveRpcProducerSetting> rpcProducerSettings;

    public EzyActiveRpcProducerManager(
        EzyEntityCodec entityCodec,
        ConnectionFactory connectionFactory,
        Map<String, EzyActiveRpcProducerSetting> rpcCallerSettings
    ) {
        super(connectionFactory);
        this.entityCodec = entityCodec;
        this.rpcProducerSettings = rpcCallerSettings;
        this.rpcProducers = createRpcCallers();
    }

    public EzyActiveRpcProducer getRpcCaller(String name) {
        EzyActiveRpcProducer caller = rpcProducers.get(name);
        if (caller == null) {
            throw new IllegalArgumentException("has no rpc caller with name: " + name);
        }
        return caller;
    }

    protected Map<String, EzyActiveRpcProducer> createRpcCallers() {
        Map<String, EzyActiveRpcProducer> map = new HashMap<>();
        for (String name : rpcProducerSettings.keySet()) {
            EzyActiveRpcProducerSetting setting = rpcProducerSettings.get(name);
            map.put(name, createRpcCaller(name, setting));
        }
        return map;
    }

    protected EzyActiveRpcProducer createRpcCaller(
        String name,
        EzyActiveRpcProducerSetting setting
    ) {
        try {
            return createRpcCaller(setting);
        } catch (Exception e) {
            throw new IllegalStateException("create rpc caller: " + name + " error", e);
        }
    }

    protected EzyActiveRpcProducer createRpcCaller(
        EzyActiveRpcProducerSetting setting
    ) throws Exception {
        Session session = getSession(setting);
        EzyActiveRpcClient client = EzyActiveRpcClient.builder()
            .session(session)
            .capacity(setting.getCapacity())
            .defaultTimeout(setting.getDefaultTimeout())
            .threadPoolSize(setting.getThreadPoolSize())
            .requestQueueName(setting.getRequestQueueName())
            .requestQueue(setting.getRequestQueue())
            .replyQueueName(setting.getReplyQueueName())
            .replyQueue(setting.getReplyQueue())
            .correlationIdFactory(setting.getCorrelationIdFactory())
            .unconsumedResponseConsumer(setting.getUnconsumedResponseConsumer())
            .build();
        return EzyActiveRpcProducer.builder()
            .entityCodec(entityCodec)
            .client(client).build();
    }

    public void close() {
        for (EzyActiveRpcProducer caller : rpcProducers.values()) {
            caller.close();
        }
    }
}
