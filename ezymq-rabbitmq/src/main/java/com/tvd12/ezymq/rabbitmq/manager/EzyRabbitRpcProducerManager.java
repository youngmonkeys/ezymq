package com.tvd12.ezymq.rabbitmq.manager;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezymq.rabbitmq.EzyRabbitRpcProducer;
import com.tvd12.ezymq.rabbitmq.constant.EzyRabbitExchangeTypes;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitRpcClient;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitRpcProducerSetting;

import java.util.HashMap;
import java.util.Map;

public class EzyRabbitRpcProducerManager
    extends EzyRabbitAbstractManager
    implements EzyCloseable {

    protected final EzyEntityCodec entityCodec;
    protected final Map<String, EzyRabbitRpcProducer> rpProducers;
    protected final Map<String, Map<String, Object>> queueArguments;
    protected final Map<String, EzyRabbitRpcProducerSetting> rpcProducerSettings;

    public EzyRabbitRpcProducerManager(
        EzyEntityCodec entityCodec,
        ConnectionFactory connectionFactory,
        Map<String, Map<String, Object>> queueArguments,
        Map<String, EzyRabbitRpcProducerSetting> rpcProducerSettings
    ) {
        super(connectionFactory);
        this.entityCodec = entityCodec;
        this.queueArguments = queueArguments;
        this.rpcProducerSettings = rpcProducerSettings;
        this.rpProducers = createRpcProducers();
    }

    public EzyRabbitRpcProducer getRpcProducer(String name) {
        EzyRabbitRpcProducer consumer = rpProducers.get(name);
        if (consumer == null) {
            throw new IllegalArgumentException("has no rpc consumer with name: " + name);
        }
        return consumer;
    }

    protected Map<String, EzyRabbitRpcProducer> createRpcProducers() {
        Map<String, EzyRabbitRpcProducer> map = new HashMap<>();
        for (String name : rpcProducerSettings.keySet()) {
            EzyRabbitRpcProducerSetting setting = rpcProducerSettings.get(name);
            map.put(name, createRpcProducer(name, setting));
        }
        return map;
    }

    protected EzyRabbitRpcProducer createRpcProducer(
        String name,
        EzyRabbitRpcProducerSetting setting
    ) {
        try {
            return createRpcProducer(setting);
        } catch (Exception e) {
            throw new IllegalStateException("create rpc consumer: " + name + " error", e);
        }
    }

    protected EzyRabbitRpcProducer createRpcProducer(
        EzyRabbitRpcProducerSetting setting
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
        return EzyRabbitRpcProducer.builder()
            .entityCodec(entityCodec)
            .client(client).build();
    }

    protected void declareComponents(
        Channel channel,
        EzyRabbitRpcProducerSetting setting
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
        for (EzyRabbitRpcProducer producer : rpProducers.values()) {
            producer.close();
        }
    }
}
