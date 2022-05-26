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
        Map<String, EzyActiveRpcProducerSetting> rpcProducerSettings
    ) {
        super(connectionFactory);
        this.entityCodec = entityCodec;
        this.rpcProducerSettings = rpcProducerSettings;
        this.rpcProducers = createRpcProducers();
    }

    public EzyActiveRpcProducer getRpcProducer(String name) {
        EzyActiveRpcProducer consumer = rpcProducers.get(name);
        if (consumer == null) {
            throw new IllegalArgumentException("has no rpc consumer with name: " + name);
        }
        return consumer;
    }

    protected Map<String, EzyActiveRpcProducer> createRpcProducers() {
        Map<String, EzyActiveRpcProducer> map = new HashMap<>();
        for (String name : rpcProducerSettings.keySet()) {
            EzyActiveRpcProducerSetting setting = rpcProducerSettings.get(name);
            map.put(name, createRpcProducer(name, setting));
        }
        return map;
    }

    protected EzyActiveRpcProducer createRpcProducer(
        String name,
        EzyActiveRpcProducerSetting setting
    ) {
        try {
            return createRpcProducer(setting);
        } catch (Exception e) {
            throw new IllegalStateException("create rpc consumer: " + name + " error", e);
        }
    }

    protected EzyActiveRpcProducer createRpcProducer(
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
        for (EzyActiveRpcProducer consumer : rpcProducers.values()) {
            consumer.close();
        }
    }
}
