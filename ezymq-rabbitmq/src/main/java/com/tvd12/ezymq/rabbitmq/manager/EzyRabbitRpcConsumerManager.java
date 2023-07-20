package com.tvd12.ezymq.rabbitmq.manager;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.rabbitmq.EzyRabbitRpcConsumer;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitRpcServer;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitRpcConsumerSetting;

import java.util.HashMap;
import java.util.Map;

import static com.tvd12.ezyfox.util.EzyProcessor.processWithLogException;

public class EzyRabbitRpcConsumerManager extends EzyRabbitAbstractManager {

    protected final EzyMQDataCodec dataCodec;
    protected final Map<String, EzyRabbitRpcConsumer> rpcConsumers;
    protected final Map<String, EzyRabbitRpcConsumerSetting> rpcConsumerSettings;

    public EzyRabbitRpcConsumerManager(
        Connection connection,
        EzyMQDataCodec dataCodec,
        Map<String, EzyRabbitRpcConsumerSetting> rpcConsumerSettings
    ) {
        super(connection);
        this.dataCodec = dataCodec;
        this.rpcConsumerSettings = rpcConsumerSettings;
        this.rpcConsumers = createRpcConsumers();
    }

    public EzyRabbitRpcConsumer getRpcConsumer(String name) {
        EzyRabbitRpcConsumer consumer = rpcConsumers.get(name);
        if (consumer == null) {
            throw new IllegalArgumentException(
                "has no rpc consumer with name: " + name
            );
        }
        return consumer;
    }

    protected Map<String, EzyRabbitRpcConsumer> createRpcConsumers() {
        Map<String, EzyRabbitRpcConsumer> map = new HashMap<>();
        for (String name : rpcConsumerSettings.keySet()) {
            EzyRabbitRpcConsumerSetting setting = rpcConsumerSettings.get(name);
            map.put(name, createRpcConsumer(name, setting));
        }
        return map;
    }

    protected EzyRabbitRpcConsumer createRpcConsumer(
        String name,
        EzyRabbitRpcConsumerSetting setting
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

    protected EzyRabbitRpcConsumer createRpcConsumer(
        EzyRabbitRpcConsumerSetting setting
    ) throws Exception {
        Channel channel = getChannel(setting);
        channel.basicQos(setting.getPrefetchCount());
        EzyRabbitRpcServer server = EzyRabbitRpcServer.builder()
            .channel(channel)
            .exchange(setting.getExchange())
            .replyRoutingKey(setting.getReplyRoutingKey())
            .queueName(setting.getRequestQueueName())
            .build();
        return EzyRabbitRpcConsumer.builder()
            .dataCodec(dataCodec)
            .requestInterceptors(setting.getRequestInterceptors())
            .requestHandlers(setting.getRequestHandlers())
            .threadPoolSize(setting.getThreadPoolSize())
            .server(server)
            .build();
    }

    public void close() {
        for (EzyRabbitRpcConsumer consumer : rpcConsumers.values()) {
            processWithLogException(consumer::close);
        }
    }
}
