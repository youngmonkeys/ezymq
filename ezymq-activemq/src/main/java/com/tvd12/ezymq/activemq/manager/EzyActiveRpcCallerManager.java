package com.tvd12.ezymq.activemq.manager;

import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezymq.activemq.EzyActiveRpcCaller;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveRpcClient;
import com.tvd12.ezymq.activemq.setting.EzyActiveRpcCallerSetting;

import javax.jms.ConnectionFactory;
import javax.jms.Session;
import java.util.HashMap;
import java.util.Map;

public class EzyActiveRpcCallerManager extends EzyActiveAbstractManager {

    protected final EzyEntityCodec entityCodec;
    protected final Map<String, EzyActiveRpcCaller> rpcCallers;
    protected final Map<String, EzyActiveRpcCallerSetting> rpcCallerSettings;

    public EzyActiveRpcCallerManager(
        EzyEntityCodec entityCodec,
        ConnectionFactory connectionFactory,
        Map<String, EzyActiveRpcCallerSetting> rpcCallerSettings
    ) {
        super(connectionFactory);
        this.entityCodec = entityCodec;
        this.rpcCallerSettings = rpcCallerSettings;
        this.rpcCallers = createRpcCallers();
    }

    public EzyActiveRpcCaller getRpcCaller(String name) {
        EzyActiveRpcCaller caller = rpcCallers.get(name);
        if (caller == null) {
            throw new IllegalArgumentException("has no rpc caller with name: " + name);
        }
        return caller;
    }

    protected Map<String, EzyActiveRpcCaller> createRpcCallers() {
        Map<String, EzyActiveRpcCaller> map = new HashMap<>();
        for (String name : rpcCallerSettings.keySet()) {
            EzyActiveRpcCallerSetting setting = rpcCallerSettings.get(name);
            map.put(name, createRpcCaller(name, setting));
        }
        return map;
    }

    protected EzyActiveRpcCaller createRpcCaller(
        String name,
        EzyActiveRpcCallerSetting setting
    ) {
        try {
            return createRpcCaller(setting);
        } catch (Exception e) {
            throw new IllegalStateException("create rpc caller: " + name + " error", e);
        }
    }

    protected EzyActiveRpcCaller createRpcCaller(
        EzyActiveRpcCallerSetting setting
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
        return EzyActiveRpcCaller.builder()
            .entityCodec(entityCodec)
            .client(client).build();
    }

    public void close() {
        for (EzyActiveRpcCaller caller : rpcCallers.values()) {
            caller.close();
        }
    }
}
