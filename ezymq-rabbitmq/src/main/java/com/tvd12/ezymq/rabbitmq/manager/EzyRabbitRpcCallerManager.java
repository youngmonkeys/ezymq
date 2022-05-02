package com.tvd12.ezymq.rabbitmq.manager;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezymq.rabbitmq.EzyRabbitRpcCaller;
import com.tvd12.ezymq.rabbitmq.constant.EzyRabbitExchangeTypes;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitRpcClient;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitRpcCallerSetting;

import java.util.HashMap;
import java.util.Map;

public class EzyRabbitRpcCallerManager
    extends EzyRabbitAbstractManager implements EzyCloseable {

    protected final EzyEntityCodec entityCodec;
    protected final Map<String, EzyRabbitRpcCaller> rpcCallers;
    protected final Map<String, Map<String, Object>> queueArguments;
    protected final Map<String, EzyRabbitRpcCallerSetting> rpcCallerSettings;

    public EzyRabbitRpcCallerManager(
        EzyEntityCodec entityCodec,
        ConnectionFactory connectionFactory,
        Map<String, Map<String, Object>> queueArguments,
        Map<String, EzyRabbitRpcCallerSetting> rpcCallerSettings
    ) {
        super(connectionFactory);
        this.entityCodec = entityCodec;
        this.queueArguments = queueArguments;
        this.rpcCallerSettings = rpcCallerSettings;
        this.rpcCallers = createRpcCallers();
    }

    public EzyRabbitRpcCaller getRpcCaller(String name) {
        EzyRabbitRpcCaller caller = rpcCallers.get(name);
        if (caller == null) {
            throw new IllegalArgumentException("has no rpc caller with name: " + name);
        }
        return caller;
    }

    protected Map<String, EzyRabbitRpcCaller> createRpcCallers() {
        Map<String, EzyRabbitRpcCaller> map = new HashMap<>();
        for (String name : rpcCallerSettings.keySet()) {
            EzyRabbitRpcCallerSetting setting = rpcCallerSettings.get(name);
            map.put(name, createRpcCaller(name, setting));
        }
        return map;
    }

    protected EzyRabbitRpcCaller createRpcCaller(
        String name,
        EzyRabbitRpcCallerSetting setting
    ) {
        try {
            return createRpcCaller(setting);
        } catch (Exception e) {
            throw new IllegalStateException("create rpc caller: " + name + " error", e);
        }
    }

    protected EzyRabbitRpcCaller createRpcCaller(
        EzyRabbitRpcCallerSetting setting
    ) throws Exception {
        Channel channel = getChannel(setting);
        declareComponents(channel, setting);
        EzyRabbitRpcClient client = EzyRabbitRpcClient.builder()
            .channel(channel)
            .exchange(setting.getExchange())
            .routingKey(setting.getRequestRoutingKey())
            .replyQueueName(setting.getReplyQueueName())
            .replyRoutingKey(setting.getReplyRoutingKey())
            .capacity(setting.getCapacity())
            .defaultTimeout(setting.getDefaultTimeout())
            .correlationIdFactory(setting.getCorrelationIdFactory())
            .unconsumedResponseConsumer(setting.getUnconsumedResponseConsumer())
            .build();
        return EzyRabbitRpcCaller.builder()
            .entityCodec(entityCodec)
            .client(client).build();
    }

    protected void declareComponents(
        Channel channel,
        EzyRabbitRpcCallerSetting setting
    ) throws Exception {
        channel.basicQos(setting.getPrefetchCount());
        channel.exchangeDeclare(
            setting.getExchange(), EzyRabbitExchangeTypes.DIRECT);
        Map<String, Object> requestQueueArguments =
            queueArguments.get(setting.getRequestQueueName());
        channel.queueDeclare(
            setting.getRequestQueueName(),
            false,
            false,
            false,
            requestQueueArguments
        );
        Map<String, Object> replyQueueArguments =
            queueArguments.get(setting.getReplyQueueName());
        channel.queueDeclare(
            setting.getReplyQueueName(),
            false,
            false,
            false,
            replyQueueArguments
        );
        channel.queueBind(
            setting.getRequestQueueName(),
            setting.getExchange(),
            setting.getRequestRoutingKey()
        );
        channel.queueBind(
            setting.getReplyQueueName(),
            setting.getExchange(),
            setting.getReplyRoutingKey()
        );
    }

    public void close() {
        for (EzyRabbitRpcCaller caller : rpcCallers.values()) {
            caller.close();
        }
    }
}
