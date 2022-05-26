package com.tvd12.ezymq.rabbitmq.manager;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.tvd12.ezymq.rabbitmq.EzyRabbitRpcConsumer;
import com.tvd12.ezymq.rabbitmq.codec.EzyRabbitDataCodec;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitRpcServer;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitRpcConsumerSetting;

import java.util.HashMap;
import java.util.Map;

public class EzyRabbitRpcConsumerManager extends EzyRabbitAbstractManager {

    protected final EzyRabbitDataCodec dataCodec;
    protected final Map<String, EzyRabbitRpcConsumer> rpcConsumers;
    protected final Map<String, EzyRabbitRpcConsumerSetting> rpcConsumerSettings;

    public EzyRabbitRpcConsumerManager(
        EzyRabbitDataCodec dataCodec,
        ConnectionFactory connectionFactory,
        Map<String, EzyRabbitRpcConsumerSetting> rpcConsumerSettings
    ) {
        super(connectionFactory);
        this.dataCodec = dataCodec;
        this.rpcConsumerSettings = rpcConsumerSettings;
        this.rpcConsumers = createRpcProducers();
    }

    public EzyRabbitRpcConsumer getRpcConsumer(String name) {
        EzyRabbitRpcConsumer handler = rpcConsumers.get(name);
        if (handler == null) {
            throw new IllegalArgumentException("has no rpc handler with name: " + name);
        }
        return handler;
    }

    protected Map<String, EzyRabbitRpcConsumer> createRpcProducers() {
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
            throw new IllegalStateException("can't create handler: " + name, e);
        }
    }

    protected EzyRabbitRpcConsumer createRpcConsumer(
        EzyRabbitRpcConsumerSetting setting
    ) throws Exception {
        Channel channel = getChannel(setting);
        channel.basicQos(setting.getPrefetchCount());
        EzyRabbitRpcServer client = EzyRabbitRpcServer.builder()
            .channel(channel)
            .exchange(setting.getExchange())
            .replyRoutingKey(setting.getReplyRoutingKey())
            .queueName(setting.getRequestQueueName())
            .build();
        EzyRabbitRpcConsumer handler = EzyRabbitRpcConsumer.builder()
            .dataCodec(dataCodec)
            .actionInterceptor(setting.getActionInterceptor())
            .requestHandlers(setting.getRequestHandlers())
            .threadPoolSize(setting.getThreadPoolSize())
            .server(client).build();
        handler.start();
        return handler;
    }

    public void close() {
        for (EzyRabbitRpcConsumer handler : rpcConsumers.values()) {
            handler.close();
        }
    }
}
