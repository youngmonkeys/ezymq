package com.tvd12.ezymq.activemq.manager;

import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezymq.activemq.EzyActiveRpcConsumer;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveRpcServer;
import com.tvd12.ezymq.activemq.setting.EzyActiveRpcConsumerSetting;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;

import javax.jms.Connection;
import javax.jms.Session;
import java.util.HashMap;
import java.util.Map;

import static com.tvd12.ezyfox.util.EzyProcessor.processWithLogException;

public class EzyActiveRpcConsumerManager
    extends EzyActiveAbstractManager
    implements EzyCloseable {

    protected final EzyMQDataCodec dataCodec;
    protected final Map<String, EzyActiveRpcConsumer> rpcConsumers;
    protected final Map<String, EzyActiveRpcConsumerSetting> rpcConsumerSettings;

    public EzyActiveRpcConsumerManager(
        Connection connection,
        EzyMQDataCodec dataCodec,
        Map<String, EzyActiveRpcConsumerSetting> rpcConsumerSettings
    ) {
        super(connection);
        this.dataCodec = dataCodec;
        this.rpcConsumerSettings = rpcConsumerSettings;
        this.rpcConsumers = createRpcConsumers();
    }

    public EzyActiveRpcConsumer getRpcConsumer(String name) {
        EzyActiveRpcConsumer consumer = rpcConsumers.get(name);
        if (consumer == null) {
            throw new IllegalArgumentException(
                "has no rpc consumer with name: " + name
            );
        }
        return consumer;
    }

    protected Map<String, EzyActiveRpcConsumer> createRpcConsumers() {
        Map<String, EzyActiveRpcConsumer> map = new HashMap<>();
        for (String name : rpcConsumerSettings.keySet()) {
            EzyActiveRpcConsumerSetting setting = rpcConsumerSettings.get(name);
            map.put(name, createRpcConsumer(name, setting));
        }
        return map;
    }

    protected EzyActiveRpcConsumer createRpcConsumer(
        String name,
        EzyActiveRpcConsumerSetting setting
    ) {
        try {
            return createRpcConsumer(setting);
        } catch (Exception e) {
            throw new IllegalStateException(
                "can't create rpc consumer: " + name,
                e
            );
        }
    }

    protected EzyActiveRpcConsumer createRpcConsumer(
        EzyActiveRpcConsumerSetting setting
    ) throws Exception {
        Session session = getSession(setting);
        EzyActiveRpcServer server = EzyActiveRpcServer.builder()
            .session(session)
            .threadPoolSize(setting.getThreadPoolSize())
            .requestQueueName(setting.getRequestQueueName())
            .requestQueue(setting.getRequestQueue())
            .replyQueueName(setting.getReplyQueueName())
            .replyQueue(setting.getReplyQueue())
            .build();
        return EzyActiveRpcConsumer.builder()
            .dataCodec(dataCodec)
            .requestInterceptors(setting.getRequestInterceptors())
            .requestHandlers(setting.getRequestHandlers())
            .server(server)
            .build();
    }

    public void close() {
        for (EzyActiveRpcConsumer consumer : rpcConsumers.values()) {
            processWithLogException(consumer::close);
        }
    }
}
