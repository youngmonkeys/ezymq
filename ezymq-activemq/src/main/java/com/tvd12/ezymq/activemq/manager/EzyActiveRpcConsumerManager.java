package com.tvd12.ezymq.activemq.manager;

import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezymq.activemq.EzyActiveRpcConsumer;
import com.tvd12.ezymq.activemq.codec.EzyActiveDataCodec;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveRpcServer;
import com.tvd12.ezymq.activemq.setting.EzyActiveRpcConsumerSetting;

import javax.jms.ConnectionFactory;
import javax.jms.Session;
import java.util.HashMap;
import java.util.Map;

public class EzyActiveRpcConsumerManager
    extends EzyActiveAbstractManager
    implements EzyCloseable {

    protected final EzyActiveDataCodec dataCodec;
    protected final Map<String, EzyActiveRpcConsumer> rpcConsumers;
    protected final Map<String, EzyActiveRpcConsumerSetting> rpcConsumerSettings;

    public EzyActiveRpcConsumerManager(
        EzyActiveDataCodec dataCodec,
        ConnectionFactory connectionFactory,
        Map<String, EzyActiveRpcConsumerSetting> rpcConsumerSettings
    ) {
        super(connectionFactory);
        this.dataCodec = dataCodec;
        this.rpcConsumerSettings = rpcConsumerSettings;
        this.rpcConsumers = createRpcCallers();
    }

    public EzyActiveRpcConsumer getRpcHandler(String name) {
        EzyActiveRpcConsumer handler = rpcConsumers.get(name);
        if (handler == null) {
            throw new IllegalArgumentException("has no rpc handler with name: " + name);
        }
        return handler;
    }

    protected Map<String, EzyActiveRpcConsumer> createRpcCallers() {
        Map<String, EzyActiveRpcConsumer> map = new HashMap<>();
        for (String name : rpcConsumerSettings.keySet()) {
            EzyActiveRpcConsumerSetting setting = rpcConsumerSettings.get(name);
            map.put(name, createRpcHandler(name, setting));
        }
        return map;
    }

    protected EzyActiveRpcConsumer createRpcHandler(
        String name,
        EzyActiveRpcConsumerSetting setting
    ) {
        try {
            return createRpcHandler(setting);
        } catch (Exception e) {
            throw new IllegalStateException("can't create handler: " + name, e);
        }
    }

    protected EzyActiveRpcConsumer createRpcHandler(
        EzyActiveRpcConsumerSetting setting
    ) throws Exception {
        Session session = getSession(setting);
        EzyActiveRpcServer client = EzyActiveRpcServer.builder()
            .session(session)
            .threadPoolSize(setting.getThreadPoolSize())
            .requestQueueName(setting.getRequestQueueName())
            .requestQueue(setting.getRequestQueue())
            .replyQueueName(setting.getReplyQueueName())
            .replyQueue(setting.getReplyQueue())
            .build();
        EzyActiveRpcConsumer handler = EzyActiveRpcConsumer.builder()
            .dataCodec(dataCodec)
            .actionInterceptor(setting.getActionInterceptor())
            .requestHandlers(setting.getRequestHandlers())
            .server(client).build();
        handler.start();
        return handler;
    }

    public void close() {
        for (EzyActiveRpcConsumer handler : rpcConsumers.values()) {
            handler.close();
        }
    }
}
